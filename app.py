import time
import os
import queue
import threading
from typing import Optional
from dotenv import load_dotenv
try:
    import torch
except ImportError:
    torch = None  # cloud-streaming-only deployment (no local ML stack)
import uvicorn
from fastapi import Depends, FastAPI, Form, Request, WebSocket, WebSocketDisconnect  # type: ignore
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session

from sql import crud, models, schemas
from sql.database import engine, SessionLocal
from routes import router
from utils.authentication import create_access_token
from utils.cookies import validate_admin_cookies, validate_cookies
from utils.logger import log_response_time, print_info, print_error, print_warning
from utils.prompts import get_products, get_prompt

# Add these imports at the top of your app.py file
from datetime import datetime, timedelta
from sqlalchemy import func, text

# Local browser-mic voice stack (torch/whisper/piper). Optional: outbound
# telephony uses the cloud streaming engine in voice/ and works without it.
try:
    from openvoicechat.tts.tts_elevenlabs import Mouth_elevenlabs
    from openvoicechat.tts.tts_xtts import Mouth_xtts
    from openvoicechat.tts.tts_piper import Mouth_piper
    from openvoicechat.llm.llm_EC2 import Chatbot_LLM as Chatbot
    from openvoicechat.stt.stt_faster_whisper import Ear_faster_whisper as Ear
    from openvoicechat.utils import run_chat, Listener_ws, Player_ws, run_chat_langchain
    LOCAL_VOICE_STACK = True
except ImportError as _local_err:
    print(f"Local voice stack unavailable ({_local_err}); "
          "browser /chatws demo disabled, outbound telephony unaffected.")
    LOCAL_VOICE_STACK = False

    class Chatbot:  # minimal stand-in so module-level chatbot init works
        def __init__(self, *args, **kwargs):
            self.messages = []


from pydantic import BaseModel
import json
import subprocess
import openai
from openai import OpenAI
import re

load_dotenv()

def get_context(self):
    return self.messages


def set_context(self, context):
    self.messages = context


Chatbot.get_context = get_context
Chatbot.set_context = set_context


SESSION_MIDDLEWARE_SECRET_KEY = os.getenv("SESSION_MIDDLEWARE_SECRET_KEY")
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
ALGORITHM = "HS256"
DEVICE = "cuda" if (torch is not None and torch.cuda.is_available()) else "cpu"

# Site crawler used for campaign knowledge ingestion. This is an external
# tool (https://github.com/BuilderIO/gpt-crawler), not part of this repo:
#   git clone https://github.com/BuilderIO/gpt-crawler && cd gpt-crawler && npm install
# Point CRAWLER_DIR elsewhere if you keep it outside the project root.
CRAWLER_DIR = os.getenv("CRAWLER_DIR", "gpt-crawler")


def crawler_installed() -> bool:
    return os.path.isdir(CRAWLER_DIR)
EAR_MODEL_ID = "openai/whisper-medium"  # "openai/whisper-small.en" or "distil-whisper/distil-large-v3"
CHATBOT_MODEL = "gpt-4o-mini"
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")

if not SESSION_MIDDLEWARE_SECRET_KEY or not JWT_SECRET_KEY:
    raise RuntimeError(
        "Missing required environment variables: SESSION_MIDDLEWARE_SECRET_KEY and JWT_SECRET_KEY"
    )

models.Base.metadata.create_all(bind=engine)

# Additive migration for pre-existing databases (voice engine columns).
from sql.database import ensure_columns
try:
    ensure_columns()
except Exception as _mig_err:
    print(f"[db] column migration warning: {_mig_err}")

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=SESSION_MIDDLEWARE_SECRET_KEY)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(router, prefix="/api")

# Outbound telephony (streaming voice engine over Twilio Media Streams).
try:
    from telephony.outbound import router as outbound_router
    app.include_router(outbound_router)
    print("Outbound calling routes registered.")
except Exception as _out_err:
    print(f"Outbound calling disabled: {_out_err}")

# Prometheus scrape target for the voice engine (aggregates only).
from voice.telemetry import telemetry as _voice_telemetry
from fastapi import Response as _MetricsResponse


@app.get("/metrics")
async def metrics():
    return _MetricsResponse(content=_voice_telemetry.render_prometheus(),
                            media_type="text/plain; version=0.0.4")

#chatbot = Chatbot(Model=CHATBOT_MODEL)
chatbot = Chatbot()


# Database Dependency Function to Manage Database Sessions
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Endpoint for Main Landing Page (GET Request)
@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("home.html", {"request": request})


# Endpoint for Signup Page (GET Request)
@app.get("/signup", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("signup.html", {"request": request})


# Endpoint for Signup Page (POST Request)
@app.post("/signup")
async def signup(
    name: str = Form(...),
    email: str = Form(...),
    phone_number: Optional[str] = Form(""),
    username: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    db: Session = Depends(get_db),
) -> JSONResponse:
    if crud.get_user(db, username):
        print_error(f'The username "{username}" is already taken.')
        return JSONResponse(
            status_code=409,
            content={
                "error": f'The username "{username}" is already taken. Please choose another one.'
            },
        )

    users = crud.get_users(db, 0, 100)
    if email in [user.email for user in users]:
        print_error(f'The email "{email}" is already taken.')
        return JSONResponse(
            status_code=409,
            content={
                "error": f'The email "{email}" is already taken. Please choose another one.'
            },
        )

    if password != confirm_password:
        error_message = "The two passwords do not match."
        print_error(error_message)
        return JSONResponse(status_code=400, content={"error": error_message})

    new_user = crud.create_user(
        db,
        schemas.UserCreate(
            name=name,
            email=email,
            phone_number=phone_number,
            username=username,
            password=password,
        ),
    )
    print_info(f"User Created: {new_user.username}")

    return JSONResponse(
        status_code=200,
        content={"response": "Your account has been created successfully!"},
    )


# Endpoint for Login Page (GET Request)
@app.get("/login", response_class=HTMLResponse)
async def login(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("login.html", {"request": request})



# Complete analytics route implementation
@app.get("/analytics")
async def analytics(request: Request, db: Session = Depends(get_db)):
    try:
        result = validate_cookies(db, request, ["current_username", "current_organization"])
        if not result["success"]:
            return result["response"]
        
        cookies = result["cookies"]
        current_username = cookies["current_username"]
        current_organization = cookies["current_organization"]
        
        # Get organization details
        organization = crud.get_organization_by_username(db, current_username)
        if not organization:
            return RedirectResponse(url="/error", status_code=404)
        
        # Get teams for this organization
        teams = crud.get_teams_by_organization_id(db, organization.id)
        
        # Initialize empty team stats list
        team_stats = []
        
        # Process each team
        for team in teams:
            # Get agents in this team
            team_agents = crud.get_agents_by_team_id(db, team.id)
            agent_stats = []
            
            team_total_calls = 0
            team_total_response_time = 0
            
            # Process each agent
            for agent in team_agents:
                # Get chat history records for this agent
                agent_chats = db.query(models.ChatHistory).filter(
                    models.ChatHistory.organization_id == organization.id,
                    models.ChatHistory.team_id == team.id,
                    models.ChatHistory.agent_id == agent.id
                ).all()
                
                # Calculate agent metrics
                calls_count = len(agent_chats)
                team_total_calls += calls_count
                
                # Get response times, filtering out None values
                response_times = [chat.response_time for chat in agent_chats if chat.response_time is not None]
                avg_response_time = sum(response_times) / len(response_times) if response_times else 0.0
                team_total_response_time += avg_response_time if calls_count > 0 else 0
                
                agent_stats.append({
                    "agent_name": agent.name,
                    "calls_count": calls_count,
                    "avg_response_time": avg_response_time
                })
            
            # Calculate team average response time
            team_avg_response_time = (
                team_total_response_time / len(team_agents) 
                if team_agents and len(team_agents) > 0 
                else 0.0
            )
            
            team_stats.append({
                "team_name": team.name,
                "team_calls": team_total_calls,
                "team_avg_response_time": team_avg_response_time,
                "agents": agent_stats
            })
        
        # Get daily call data for charts (last 7 days)
        daily_calls = []
        today = datetime.now().date()
        
        # Generate data for last 7 days
        for i in range(6, -1, -1):
            date = today - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            
            # For SQLite, use string comparison for date filtering
            day_start = date.strftime('%Y-%m-%d 00:00:00')
            day_end = date.strftime('%Y-%m-%d 23:59:59')
            
            try:
                # Count chats where timestamp is between day_start and day_end
                day_chats = db.query(models.ChatHistory).filter(
                    models.ChatHistory.organization_id == organization.id,
                    models.ChatHistory.timestamp >= day_start,
                    models.ChatHistory.timestamp <= day_end
                ).count()
            except Exception as e:
                print(f"Error querying for date {date_str}: {e}")
                # Fallback to direct string comparison if needed
                try:
                    # Alternative approach using substring extraction
                    date_substr = date_str + "%"  # Using % as wildcard
                    day_chats = db.query(models.ChatHistory).filter(
                        models.ChatHistory.organization_id == organization.id,
                        models.ChatHistory.timestamp.like(date_substr)
                    ).count()
                except Exception as e2:
                    print(f"Secondary error querying for date {date_str}: {e2}")
                    day_chats = 0
            
            daily_calls.append({
                "date": date_str, 
                "calls": day_chats
            })
        
        return templates.TemplateResponse(
            "analytics.html",
            {
                "request": request,
                "organization": organization,
                "team_stats": team_stats,
                "daily_calls": daily_calls
            }
        )
    except Exception as e:
        # Log the error and return a friendly error page
        print(f"Error in analytics route: {e}")
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "error_message": "An error occurred while loading analytics. Please try again later."
            }
        )


# Endpoint for Login Page (POST Request)
@app.post("/login")
async def login(
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
) -> JSONResponse:
    if crud.authenticate_user(db, username, password):
        redirect_url = "/admin-dashboard" if username == "admin" else "/user-dashboard"
        token = create_access_token(username)
        print_info(f"User Logged In: {username}")
        response = JSONResponse(
            status_code=200,
            content={
                "response": "Success",
                "redirect_url": redirect_url,
                "token": token,
            },
        )
        response.set_cookie(key="access_cookie", value=token, httponly=True)
        response.set_cookie(
            key="current_username", value=username, httponly=True
        )  # Store the Current Username in the Cookies
        organization = crud.get_organization_by_username(db, username)
        if organization:
            response.set_cookie(
                key="current_organization", value=organization.name
            )  # Store the Current Organization in the Cookies
        return response
    else:
        error_message = "Invalid credentials! Please check your username and/or password and try again."
        print_error(error_message)
        return JSONResponse(
            status_code=401,
            content={"error": error_message},
        )


# Endpoint for Admin Dashboard Page (GET Request)
@app.get("/admin-dashboard")
async def admin_dashboard(request: Request, db: Session = Depends(get_db)):
    result = validate_admin_cookies(db, request)
    if not result["success"]:
        return result["response"]

    users = crud.get_users(db, 0, 100)
    users = [user for user in users if user.username != "admin"]
    organizations = crud.get_organizations(db, 0, 100)
    teams = {
        organization.name: crud.get_teams_by_organization_id(db, organization.id)
        for organization in organizations
    }
    agents = {
        organization.name: {
            team.name: crud.get_agents_by_team_id(db, team.id)
            for team in teams[organization.name]
        }
        for organization in organizations
    }

    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "users": users,
            "organizations": organizations,
            "teams": teams,
            "agents": agents,
        },
    )


# Endpoint for User Dashboard Page (GET Request)
@app.get("/user-dashboard")
async def user_dashboard(request: Request, db: Session = Depends(get_db)):
    result = validate_cookies(db, request, ["current_username"])
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
    else:
        return result["response"]

    organization = crud.get_organization_by_username(db, current_username)
    if not organization:
        teams = agents = None
    else:
        teams = crud.get_teams_by_organization_id(db, organization.id)
        if not teams:
            agents = None
        else:
            agents = {
                team.name: crud.get_agents_by_team_id(db, team.id) for team in teams
            }

    return templates.TemplateResponse(
        "user_dashboard.html",
        {
            "request": request,
            "organization": organization,
            "teams": teams,
            "agents": agents,
        },
    )


# Endpoint for Organization Form (GET Request)
@app.get("/create-organization", response_class=HTMLResponse)
async def create_organization(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("create_organization.html", {"request": request})


# Endpoint to Create an Organization (POST Request)
@app.post("/create-organization", response_class=HTMLResponse)
async def create_organization(
    request: Request,
    organization_name: str = Form(...),
    organization_description: Optional[str] = Form(""),
    target_audience: Optional[str] = Form(""),
    products: Optional[list] = Form([]),
    other_details: Optional[str] = Form(""),
    db: Session = Depends(get_db),
):
    form_data = await request.form()
    products = get_products(form_data)

    # Get current username from cookies
    result = validate_cookies(db, request, ["current_username"])
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
    else:
        return result["response"]

    # Check if organization already exists
    if crud.get_organization_by_organization_name(db, organization_name):
        print_error(f'Organization "{organization_name}" already exists.')

        # Send error response back to frontend
        error_message = f'The organization "{organization_name}" already exists. Please try again with a different name.'
        return HTMLResponse(content=f"<script>window.alert('{error_message}'); window.history.back();</script>", status_code=400)

    # Access the user's JSON file
    json_file_path = os.path.join(CRAWLER_DIR, f"{current_username}-1.json")
    if os.path.exists(json_file_path) and os.path.getsize(json_file_path) > 0:
        try:
            with open(json_file_path, "r", encoding="utf-8") as json_file:  # Specify encoding
                json_content = json.load(json_file)
                
            # Append the JSON content to 'other_details'
            other_details += f"\n\n---\nBusiness Details from website:\n{json.dumps(json_content, indent=2)}"
        except json.JSONDecodeError:
            print("Error: The JSON file is invalid or could not be decoded.")
        except UnicodeDecodeError:
            print("Error: The JSON file contains invalid characters.")

    # Create new organization in the database
    new_organization = crud.create_organization(
        db,
        schemas.OrganizationCreate(
            name=organization_name,
            description=organization_description,
            target_audience=target_audience,
            products=products,
            other_details=other_details,
        ),
    )
    
    # Update user with the new organization ID
    crud.update_user(
        db, current_username, schemas.UserUpdate(organization_id=new_organization.id)
    )
    print_info(f'Organization Created: "{organization_name}"')

    # Set the 'current_organization' cookie and redirect to the dashboard
    response = RedirectResponse(url="/user-dashboard", status_code=303)
    response.set_cookie(
        key="current_organization", value=organization_name, httponly=True
    )
    
    return response


# Endpoint for Team Form (GET Request)
@app.get("/team-details")
async def team_details(request: Request, db: Session = Depends(get_db)):
    result = validate_cookies(db, request, ["current_organization"])
    if result["success"]:
        cookies = result["cookies"]
        current_organization = cookies["current_organization"]
    else:
        return result["response"]

    organization = crud.get_organization_by_organization_name(db, current_organization)
    products = organization.products if organization else None

    return templates.TemplateResponse(
        "create_team.html",
        {"request": request, "products": products},
    )


## Crawl output for the logged-in user (replaces the old static mount of
## the whole crawler directory, which also exposed its source and config).
@app.get("/crawler-output")
async def crawler_output(request: Request, db: Session = Depends(get_db)):
    result = validate_cookies(db, request, ["current_username"])
    if not result["success"]:
        return result["response"]
    current_username = result["cookies"]["current_username"]

    json_file_path = os.path.join(CRAWLER_DIR, f"{current_username}-1.json")
    if not os.path.exists(json_file_path):
        return JSONResponse(status_code=404,
                            content={"error": "No crawl output found."})
    with open(json_file_path, "r", encoding="utf-8") as f:
        return JSONResponse(content=json.load(f))


@app.get("/display-output", response_class=HTMLResponse)
async def display_output(request: Request, db: Session = Depends(get_db)):
    result = validate_cookies(db, request, ["current_username"])
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
    else:
        return result["response"]
    
    try:
        crud.get_user(db, current_username)
    except:
        return JSONResponse(
            status_code=400,
            content={"error": "User not found. Please try again"}
        )
    
    # Pass the current_username to the HTML template
    return templates.TemplateResponse("display-output.html", {"request": request, "current_username": current_username})




# Define the model for the request body
class ConfigUpdate(BaseModel):
    url: str
    maxPages: int


@app.post("/update-config")
async def update_config(config_update: ConfigUpdate, request: Request, db: Session = Depends(get_db)):
    result = validate_cookies(db, request, ["current_username"])
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
    else:
        return result["response"]
    if not crawler_installed():
        return {"success": False,
                "error": f"Crawler not installed at '{CRAWLER_DIR}'. "
                         "See CRAWLER_DIR note in app.py."}
    try:
        crud.get_user(db, current_username)
        # Path to the config.ts file in the crawler directory
        config_file_path = os.path.join(CRAWLER_DIR, "config.ts")

        # Read the current config.ts file content
        with open(config_file_path, "r") as f:
            config_data = f.readlines()

        # Modify the necessary lines in the config file
        for i, line in enumerate(config_data):
            if "url:" in line:
                config_data[i] = f'  url: "{config_update.url}",\n'
            elif "match:" in line:
                # Update the match field to reflect the new url
                config_data[i] = f'  match: "{config_update.url}/**",\n'
            elif "maxPagesToCrawl:" in line:
                config_data[i] = f"  maxPagesToCrawl: {config_update.maxPages},\n"
            elif "outputFileName:" in line:
                config_data[i] = f'  outputFileName: "{current_username}.json",\n'

        # Write the updated content back to config.ts
        with open(config_file_path, "w") as f:
            f.writelines(config_data)
        print("Successful write to config.ts")
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/start-crawl")
async def start_crawl():
    if not crawler_installed():
        return {"success": False,
                "error": f"Crawler not installed at '{CRAWLER_DIR}'. "
                         "See CRAWLER_DIR note in app.py."}
    try:
        # Run npm start inside the crawler directory
        result = subprocess.run(
            "npm start", cwd=CRAWLER_DIR,
            shell=True, capture_output=True, text=True
        )

        # Check if the crawler ran successfully
        if result.returncode == 0:
            print("Crawler ran successfully")
            return {"success": True}
        else:
            print("Crawler failed to run D:", result.stderr)
            return {"success": False, "error": result.stderr}
    except Exception as e:
        print("Exception occurred while running the crawler:", e)
        return {"success": False, "error": str(e)}


def clean_json_content(raw_content):
    # Replace unwanted characters specifically
    cleaned_content = raw_content

    # Use regex to replace unwanted characters
    cleaned_content = re.sub(r'Â', '', cleaned_content)  # Remove "Â"
    cleaned_content = re.sub(r'\xa0', ' ', cleaned_content)  # Replace non-breaking space
    cleaned_content = re.sub(r'[\u00A0]', ' ', cleaned_content)  # Ensure no non-breaking spaces remain

    # Use regex to remove any other unwanted characters and ensure words are intact
    cleaned_content = re.sub(r'[^\x00-\x7F]+', ' ', cleaned_content)  # Remove non-ASCII characters
    cleaned_content = re.sub(r'\s+', ' ', cleaned_content)  # Replace multiple spaces with a single space
    cleaned_content = cleaned_content.strip()  # Remove leading/trailing whitespace
    return cleaned_content


def condense_json(file_path):
    # Load environment variables, including the API key
    print("Beginning condense_json function.")
    api_key = os.getenv("LLM_EC2_KEY")
    client = OpenAI()
    if not api_key:
        raise ValueError("EC2 API key not found. Please check your environment variables.")
    
    openai.api_key = api_key  # Set the API key for the OpenAI module

    # Load the JSON file
    json_data = None  # Initialize json_data
    try:
        with open(file_path, 'r', encoding='utf-8') as file:  # Use utf-8 encoding
            raw_json_data = file.read()  # Read raw content as string
        
        # Clean the JSON content
        cleaned_json_content = clean_json_content(raw_json_data)

        # Load cleaned JSON string into a dictionary
        json_data = json.loads(cleaned_json_content)  # Parse cleaned JSON content
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {str(e)}")  # Log JSON decode errors
        return None
    except UnicodeDecodeError as e:  # Handle encoding errors
        print(f"Error reading JSON file due to encoding: {str(e)}")
        return None
    except Exception as e:
        print(f"Error reading JSON file: {str(e)}")
        return None

    if json_data is None:
        print("No valid JSON data found. Exiting function.")
        return None

    # Construct the prompt
    prompt = f"""You are an intelligent assistant designed to process website data extracted by a web crawler. The attached JSON file contains the full structure and details of a business's website, which we need you to condense very precisely and accurately. Your task is to read through the file, identify the most critical details (such as page titles, key sections, and important services offered), and summarize them in a structured format. Please ensure the following in your response: 
                1. Title of each page or section: Identify and list the title of every significant page or section within the website. 
                2. Services Summarization: If the section describes services, pricing, or memberships, provide a brief summary of requirements and focus areas without losing essential context. 
                3. Conciseness: Ensure that the entire output is highly condensed, removing any repetitive or non-critical information, while still maintaining full accuracy. 
                4. Exactness: The summarized information must be precise and correct, representing the original content of the output.json accurately, as if no information is lost in the condensation.
                Structure:
                - Each title should be a heading.
                - Each title should be followed by key details which should be listed under a "Details" subheading, with a short description.
                Do not omit any important details and ensure the formatting is clean and easy to read, remove all \n and #'s. The summarized output should be clear, accurate, and directly usable. Do not add any titles or URLs at all or any additional text besides what is stated in this template.
                Please proceed by reading the attached JSON file and condense its details in the manner specified. Do not exceed 500 words in any case. \n
                Please condense this data: {json.dumps(json_data)}"""

    # Send the prompt to GPT for condensation
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )

    # Extract the condensed output
    condensed_output = completion.choices[0].message.content

    # Write the condensed output back to the original JSON file
    with open(file_path, 'w', encoding='utf-8') as file:  # Use utf-8 encoding
        json.dump(condensed_output, file)
    print("Output replaced.")
    return True


@app.get("/condense-output")
async def condense_output(request: Request, db: Session = Depends(get_db)):
    print("Received request to condense output.")  # Log request initiation
    result = validate_cookies(db, request, ["current_username"])
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
        print(f"Current username retrieved: {current_username}")  # Log username
    else:
        print("User validation failed.")  # Log validation failure
        return result["response"]

    try:
        # Ensure the user exists in the database
        user = crud.get_user(db, current_username)
        print(f"User {current_username} exists in the database.")  # Log user existence
    except Exception as e:
        print(f"Error retrieving user: {str(e)}")  # Log error details
        return JSONResponse(
            status_code=400,
            content={"error": "User not found. Please try again"}
        )

    # Construct the file path for the user-specific JSON file
    json_file_path = os.path.join(CRAWLER_DIR, f"{current_username}-1.json")
    print(f"Sending {json_file_path} to be condensed.")  # Log file path

    try:
        # Condense the JSON content using GPT
        success = condense_json(json_file_path)
        if success:
            print("JSON content condensed and saved successfully.")  # Log successful condensation
            return JSONResponse(
                status_code=200,
                content={"message": "JSON file processed and updated successfully."}
            )
        else:
            return JSONResponse(
                status_code=500,
                content={"error": "An error occurred while condensing the JSON file."}
            )
    except FileNotFoundError:
        print(f"File {json_file_path} not found.")  # Log file not found
        return JSONResponse(
            status_code=404,
            content={"error": f"File {current_username}-1.json not found."}
        )
    except Exception as e:
        print(f"An error occurred while processing the file: {str(e)}")  # Log error details
        return JSONResponse(
            status_code=500,
            content={"error": "An error occurred while processing the file."}
        )


# Endpoint to Create a Team (POST Request)
@app.post("/create-team")
async def create_team(
    request: Request,
    team_name: str = Form(...),
    campaign_goals: Optional[str] = Form(""),
    selected_products: Optional[list[str]] = Form([""]),
    db: Session = Depends(get_db),
):
    result = validate_cookies(db, request, ["current_organization"])
    if result["success"]:
        cookies = result["cookies"]
        current_organization = cookies["current_organization"]
    else:
        return result["response"]

    organization = crud.get_organization_by_organization_name(db, current_organization)
    existing_team = crud.get_team_by_team_name_and_organization_id(
        db, team_name, organization.id
    )
    if existing_team:
        print_error(f'Team "{existing_team.name}" already exists.')
        return JSONResponse(
            status_code=400,
            content={
                "error": f'Team "{existing_team.name}" already exists. Please try again with a different name.'
            },
        )

    new_team = crud.create_team(
        db,
        schemas.TeamCreate(
            name=team_name,
            campaign_goals=campaign_goals,
            selected_products=selected_products,
            organization_id=organization.id,
        ),
    )
    print_info(f"Team Created: {new_team.name}")

    return JSONResponse(
        status_code=201,
        content={"response": f'Team "{new_team.name}" has been created successfully!'},
    )



# Endpoint to Delete a Team (DELETE Request)
@app.delete("/delete-team/{team_name}")
async def delete_team(request: Request, team_name: str, db: Session = Depends(get_db)):
    result = validate_cookies(db, request, ["current_username", "current_organization"])
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
        current_organization = cookies["current_organization"]
    else:
        return result["response"]

    organization = (
        crud.get_organization_by_organization_name(db, current_organization)
        if crud.get_user(db, current_username)
        else None
    )
    team_to_delete = crud.get_team_by_team_name_and_organization_id(
        db, team_name, organization.id
    )
    if not team_to_delete:
        error_message = f'Team "{team_name}" does not exist.'
        print_error(error_message)
        return JSONResponse(status_code=404, content={"error": error_message})
    else:
        print_info(f"Team Deleted: {team_to_delete.name}")
        crud.delete_team(db, team_to_delete.id)
        return JSONResponse(
            status_code=200,
            content={"response": f'Team "{team_name}" has been deleted successfully.'},
        )


# Endpoint for Agent Form (POST Request)
@app.post("/agent-details", response_class=HTMLResponse)
async def agent_details(request: Request, team_name: str = Form(...)):
    response = templates.TemplateResponse("create_agent.html", {"request": request})
    response.set_cookie(
        key="current_team", value=team_name, httponly=True
    )  # Store the Current Team Name in the Cookies
    return response


# Endpoint to Create an Agent (POST Request)
@app.post("/create-agent")
async def create_agent(
    request: Request,
    agent_name: str = Form(...),
    agent_function: str = Form(...),
    forwarding_criteria: Optional[str] = Form(""),
    departments: Optional[list[str]] = Form([""]),
    other_department: Optional[str] = Form(""),
    use_elevenlabs: Optional[str] = Form("false"),
    voice_id: Optional[str] = Form(""),
    db: Session = Depends(get_db),
):

    if (
        use_elevenlabs == False
        or use_elevenlabs == "false"
        or use_elevenlabs == "False"
    ):
        voice_id = None
    result = validate_cookies(db, request, ["current_username", "current_team"])
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
        current_team = cookies["current_team"]
    else:
        return result["response"]

    user = crud.get_user(db, current_username)
    organization = user.organization if user else None
    team = crud.get_team_by_team_name_and_organization_id(
        db, current_team, organization.id
    )

    departments = ", ".join(departments)
    use_elevenlabs = use_elevenlabs.lower() == "on" or use_elevenlabs == "true"

    print(f"use_elevenlabs: {use_elevenlabs}")
    print(f"voice_id: {voice_id}")

    new_agent = crud.create_agent(
        db,
        schemas.AgentCreate(
            name=agent_name,
            team_id=team.id,
            agent_function=agent_function,
            forwarding_criteria=forwarding_criteria,
            departments=departments,
            other_department=other_department,
            use_elevenlabs=use_elevenlabs,
            voice_id=voice_id,
        ),
    )

    print_info(f"Agent Created: {new_agent.name}")
    response = RedirectResponse(url="/user-dashboard", status_code=303)
    # if use_elevenlabs:
    #     response.set_cookie(key="voice_id", value=voice_id, httponly=True)

    # if not use_elevenlabs:
    #     response.set_cookie(key="voice_id", value=None, httponly=True)
    return response


# Endpoint to Delete an Agent (DELETE Request)
@app.get("/delete-agent/{agent_name}", response_class=HTMLResponse)
async def delete_agent(
    agent_name: str, request: Request, db: Session = Depends(get_db)
):
    result = validate_cookies(
        db, request, ["current_username", "current_organization", "current_team"]
    )
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
        current_organization = cookies["current_organization"]
        current_team = cookies["current_team"]
    else:
        return result["response"]

    user = crud.get_user(db, current_username)
    organization = user.organization if user else None
    team = crud.get_team_by_team_name_and_organization_id(
        db, current_team, organization.id
    )
    agent_to_delete = crud.get_agent_by_agent_name_and_team_id_and_organization_id(
        db, agent_name, team.id, organization.id
    )
    if not agent_to_delete:
        error_message = f'Agent "{agent_name}" does not exist.'
        print_error(error_message)
        return JSONResponse(status_code=404, content={"error": error_message})
    else:
        print_info(f"Agent Deleted: {agent_to_delete.name}")
        crud.delete_agent(db, agent_to_delete.id)
        return JSONResponse(
            status_code=200,
            content={
                "response": f'Agent "{agent_name}" has been deleted successfully.'
            },
        )


# Endpoint to Use an Agent
@app.get("/use-agent/{team_name}/{agent_name}", response_class=HTMLResponse)
async def use_agent(
    request: Request, team_name: str, agent_name: str, db: Session = Depends(get_db)
):
    result = validate_cookies(
        db, request, ["current_username", "current_organization", "current_team"]
    )
    if result["success"]:
        cookies = result["cookies"]
        current_username = cookies["current_username"]
        current_organization = cookies["current_organization"]
        
    else:
        return result["response"]
    print("Use Agent Called")
    organization = crud.get_organization_by_username(db, current_username)
    # if not organization:
    #     return RedirectResponse(url="/error", status_code=404)
    teams = crud.get_teams_by_organization_id(db, organization.id)
    teams_dict = {team.name: team for team in teams}
    # if team_name not in teams_dict:
    #     return RedirectResponse(url="/error", status_code=404)
    agents = crud.get_agents_by_team_id(db, teams_dict[team_name].id)
    agents_dict = {agent.name: agent for agent in agents}
    # if agent_name not in agents_dict:
    #     return RedirectResponse(url="/error", status_code=404)

    team = teams_dict[team_name]
    agent = agents_dict[agent_name]
    prompt = get_prompt(organization, team, agent)
    print(prompt)

    existing_chat = crud.get_chat_history_by_organization_team_agent(
        db, organization.id, teams_dict[team_name].id, agent.id
    )
    # if existing_chat:
    #     existing_chat.chat_data.append({"role": "system", "content": prompt})
    #     db.commit()
    #     db.refresh(existing_chat)
    # else:
    #     crud.create_chat_history(
    #         db,
    #         schemas.ChatHistoryCreate(
    #             organization_id=organization.id,
    #             team_id=teams_dict[team_name].id,
    #             agent_id=agent.id,
    #             chat_data=[{"role": "system", "content": prompt}],
    #             response_time=0.0,
    #         ),
    #     )
    if existing_chat:
        # Clear the previous chat data for this session
        existing_chat.chat_data = [{"role": "system", "content": prompt}]
        db.commit()
        db.refresh(existing_chat)
    else:
        crud.create_chat_history(
            db,
            schemas.ChatHistoryCreate(
                organization_id=organization.id,
                team_id=teams_dict[team_name].id,
                agent_id=agent.id,
                chat_data=[{"role": "system", "content": prompt}],
                response_time=0.0,
            ),
        )

    response = RedirectResponse(url="/chat", status_code=303)
    response.set_cookie(key="current_team", value=team_name, httponly=True)
    response.set_cookie(key="current_agent", value=agent_name, httponly=True)
    return response


# Endpoint to Chat with an Agent
@app.get("/chat", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("stream_audio.html", {"request": request})


# Endpoint to Chat with an Agent (WebSocket)
@app.websocket("/chatws")
async def websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    await websocket.accept()

    if not LOCAL_VOICE_STACK:
        await websocket.close(
            code=1011,
            reason="Local voice stack not installed (browser demo needs "
                   "requirements.txt); outbound telephony is unaffected.")
        return
    print("WebSocket Connected.")

    cookie_header = websocket.headers.get("cookie")
    if not cookie_header:
        await websocket.close(code=1008, reason="No cookies found in headers.")
        return

    cookies = {
        cookie.split("=")[0]: cookie.split("=")[1]
        for cookie in cookie_header.split("; ")
    }

    current_username = cookies.get("current_username")
    current_organization = cookies.get("current_organization")
    current_team = cookies.get("current_team").replace('"', "")
    current_agent = cookies.get("current_agent").replace('"', "")

    if not all([current_organization, current_team, current_agent]):
        await websocket.close(code=1008, reason="Incomplete session data.")
        return

    organization = crud.get_organization_by_username(db, current_username)

    teams = crud.get_teams_by_organization_id(db, organization.id)
    teams_dict = {team.name: team for team in teams}
    team = teams_dict.get(current_team)
    if not team:
        await websocket.close(code=1008, reason="Team not found.")
        return

    agents = crud.get_agents_by_team_id(db, team.id)
    agents_dict = {agent.name: agent for agent in agents}
    agent = agents_dict.get(current_agent)
    if not agent:
        await websocket.close(code=1008, reason="Agent not found.")
        return

    existing_chat = crud.get_chat_history_by_organization_team_agent(
        db, organization.id, team.id, agent.id
    )
    if existing_chat:
        print_info("Setting Chatbot context")
        print(existing_chat.chat_data)
        chatbot.set_context(existing_chat.chat_data)
        
    # Configure the chatbot to produce shorter responses
    if hasattr(chatbot, 'max_tokens'):
        chatbot.max_tokens = 75  # Set lower token limit for all agents
        
    # Increase the temperature slightly for more focused responses
    if hasattr(chatbot, 'temperature'):
        chatbot.temperature = 0.7
        
    input_queue = queue.Queue()
    output_queue = queue.Queue()
    listener = Listener_ws(input_queue)
    player = Player_ws(output_queue)

    voice_id = agent.voice_id
    print(f"Current Voice ID: {voice_id}")
    if voice_id != None and voice_id != "None":  # Use ElevenLabs TTS
        print("Using ElevenLabs TTS")
        start = time.time()
        #mouth = Mouth_elevenlabs(voice_id=voice_id, player=player)
        mouth = Mouth_piper(player=player, device=DEVICE)
        end = time.time()
        log_response_time("TTS Model (ElevenLabs) Loading Time", round(end - start, 3))
    elif voice_id == "None" or voice_id == None:  # Use piper
        start = time.time()
        mouth = Mouth_piper(player=player, device=DEVICE)
        end = time.time()
        log_response_time("TTS Model (Piper) Loading Time", round(end - start, 3))

    start = time.time()
    # ear = Ear(
    #     api_key=DEEPGRAM_API_KEY,
    #     silence_seconds=2.0,
    #     listener=listener,
    # )

    ear = Ear(
    model_size="distil-large-v3",  # You can choose different sizes based on your needs
    device=DEVICE,
    compute_type="float16" if DEVICE == "cuda" else "int8",
    silence_seconds=2.0,
    listener=listener,
    stream=True,  # Enable streaming mode for better timing
    player=player
)
    end = time.time()
    log_response_time(f"STT Model Loading Time", round(end - start, 3))


    metrics = {
    "recording_times": [],
    "stt_times": [],
    "llm_times": [],
    "tts_times": [],
    "total_cycle_times": []
    }

    # Create a custom callback to track timing metrics
    def timing_callback(phase, start_time, end_time):
        duration = round(end_time - start_time, 3)
        if phase == "stt":
            metrics["stt_times"].append(duration)
            log_response_time("STT Processing Time", duration)
        elif phase == "llm":
            metrics["llm_times"].append(duration)
            log_response_time("LLM Response Time", duration)
        elif phase == "tts":
            metrics["tts_times"].append(duration)
            log_response_time("TTS Processing Time", duration)
        elif phase == "total_cycle":
            metrics["total_cycle_times"].append(duration)
            log_response_time("Total Cycle Time", duration)


    product_names = []
    product_descriptions = []
    product_features = []

    for product in organization.products:
        product_names.append(product["name"])
        product_descriptions.append(product["description"])
        product_features.append(product["feature"])

    minibot_args = {
        "agent_name": agent.name,
        "organization_name": organization.name,
        "total_products": len(organization.products),
        "product_names": product_names,
        "product_descriptions": product_descriptions,
        "product_features": product_features,
    }

    # Check if this is an information collection agent
    is_info_agent = agent.agent_function.lower() == "information"

    if is_info_agent:
        # Import the information collection module
        from openvoicechat.agent_util import run_chat_agent
        print("Running INFO AGENT")

        # Define save path for collected information (organization and team specific)
        save_path = f"collected_info_{organization.name}_{team.name}.csv"
        
        # Use information collection prompt
        from utils.prompts import get_agent_information_prompt
        info_prompt = get_agent_information_prompt(organization, team, agent)
        chatbot.messages = [{"role": "system", "content": info_prompt}]
        
        # Start the information collection agent thread
        threading.Thread(
            target=run_chat_agent,
            args=(
                mouth, 
                ear, 
                chatbot, 
                minibot_args, 
                True,  # verbose
                lambda x: False,  # stopping_criteria
                True,  # starting_message
                "chat_log.txt",  # logging_path
                save_path,  # save_path for collected information
                timing_callback  # Pass the timing callback
            )
        ).start()
    else:
        # Use regular chat for non-information agents
        threading.Thread(
            target=run_chat_langchain, 
            args=(mouth, ear, chatbot, minibot_args, True)
        ).start()

    try:
        while True:
            data = await websocket.receive_bytes()
            if listener.listening:
                input_queue.put(data)
            if not output_queue.empty():
                response_data = output_queue.get_nowait()
            else:
                response_data = "none".encode()
            await websocket.send_bytes(response_data)
            try:
                existing_chat.chat_data = chatbot.get_context()

                if metrics["total_cycle_times"]:
                    latency_data = {
                        "avg_stt_time": sum(metrics["stt_times"]) / len(metrics["stt_times"]) if metrics["stt_times"] else 0,
                        "avg_llm_time": sum(metrics["llm_times"]) / len(metrics["llm_times"]) if metrics["llm_times"] else 0,
                        "avg_tts_time": sum(metrics["tts_times"]) / len(metrics["tts_times"]) if metrics["tts_times"] else 0,
                        "avg_total_time": sum(metrics["total_cycle_times"]) / len(metrics["total_cycle_times"]) if metrics["total_cycle_times"] else 0,
                    }

                    existing_chat.response_time = latency_data["avg_total_time"]
                    existing_chat.metrics = latency_data  # You'll need to add this field to your model
                
                db.commit()
                db.refresh(existing_chat)
            except:
                ...
    except WebSocketDisconnect:
        print("WebSocket Disconnected.")
        del mouth
        del ear
        torch.cuda.empty_cache()
    finally:
        await websocket.close()
        torch.cuda.empty_cache()


# Endpoint to Get Chat History of an Agent
@app.get("/get-chat-history", response_class=HTMLResponse)
async def get_chat_history(request: Request, db: Session = Depends(get_db)):
    # Validate cookies and check for necessary values
    result = validate_cookies(db, request, ["current_username", "current_organization", "current_team", "current_agent"])
    
    if not result["success"]:
        return result["response"]

    cookies = result["cookies"]
    current_username = cookies["current_username"]
    current_organization = cookies["current_organization"]
    current_team = cookies["current_team"].replace('"', "")
    current_agent = cookies["current_agent"].replace('"', "")
    
    # Retrieve the user based on the current username stored in cookies
    user = crud.get_user(db, current_username)
    
    if user is None:
        return HTMLResponse(content="<h1>User not found</h1>", status_code=404)

    # Retrieve the organization of the user
    organization = user.organization

    # Ensure the organization exists
    if organization is None:
        return HTMLResponse(content="<h1>Organization not found</h1>", status_code=404)

    # Ensure the organization matches the one from the cookies
    if organization.name != current_organization:
        return HTMLResponse(content="<h1>Organization mismatch</h1>", status_code=403)

    # Get teams related to the organization
    teams = crud.get_teams_by_organization_id(db, organization.id)
    teams_dict = {team.name: team for team in teams}

    # Check if the team exists
    if current_team not in teams_dict:
        return HTMLResponse(content="<h1>Team not found</h1>", status_code=404)

    team = teams_dict[current_team]

    # Get agents related to the team
    agents = crud.get_agents_by_team_id(db, team.id)
    agents_dict = {agent.name: agent for agent in agents}

    # Check if the agent exists
    if current_agent not in agents_dict:
        return HTMLResponse(content="<h1>Agent not found</h1>", status_code=404)

    agent = agents_dict[current_agent]

    # Retrieve the chat history based on organization, team, and agent
    chat_history = crud.get_chat_history_by_organization_team_agent(
        db, organization.id, team.id, agent.id
    )
    
    # Ensure chat_history exists before trying to access chat_data
    if chat_history is None:
        return HTMLResponse(content="<h1>No chat history found</h1>", status_code=404)

    # Debugging output
    print("Team: ", team.name)
    print(f"Agent: {agent.name}")
    #print("Chat History:", chat_history) 
    chat_history_list = chat_history.chat_data  # Assuming chat_data is directly accessible
    system_prompt = f"Hey, there! You're speaking with {agent.name} from {current_organization}. How can I assist you?"

    return templates.TemplateResponse(
        "chat_history.html", {"request": request, "chat_history": chat_history_list, "system_prompt": system_prompt} 
    )



# Running the FastAPI Application with Uvicorn
if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        ssl_keyfile="keys/key.pem",
        ssl_certfile="keys/cert.pem",
    )
