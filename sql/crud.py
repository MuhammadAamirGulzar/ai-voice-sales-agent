from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from . import models, schemas
from werkzeug.security import check_password_hash, generate_password_hash


# User CRUD Operations
def create_user(db: Session, user: schemas.UserCreate):
    new_user = models.User(
        name=user.name,
        email=user.email,
        phone_number=user.phone_number,
        username=user.username,
        password=generate_password_hash(user.password),
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


def get_user(db: Session, username: str):
    return db.query(models.User).filter(models.User.username == username).first()


def get_users(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.User).offset(skip).limit(limit).all()


def update_user(db: Session, username: str, user: schemas.UserUpdate):
    db_user = db.query(models.User).filter(models.User.username == username).first()
    if db_user:
        if user.name is not None:
            db_user.name = user.name
        if user.email is not None:
            db_user.email = user.email
        if user.phone_number is not None:
            db_user.phone_number = user.phone_number
        if user.username is not None:
            db_user.username = user.username
        if user.password is not None:
            db_user.password = generate_password_hash(user.password)
        if user.organization_id is not None:
            db_user.organization_id = user.organization_id
        db.commit()
        db.refresh(db_user)
        return db_user
    return None


def delete_user(db: Session, username: str):
    user = db.query(models.User).filter(models.User.username == username).first()
    if user:
        db.delete(user)
        db.commit()
        return user
    return None


def authenticate_user(db: Session, username: str, password: str):
    try:
        user = db.query(models.User).filter(models.User.username == username).one()
        if user and check_password_hash(user.password, password):
            return user
    except NoResultFound:
        return None
    return None


# Organization CRUD Operations
def create_organization(db: Session, organization: schemas.OrganizationCreate):
    new_organization = models.Organization(
        name=organization.name,
        description=organization.description,
        target_audience=organization.target_audience,
        products=organization.products,
        other_details=organization.other_details,
    )
    db.add(new_organization)
    db.commit()
    db.refresh(new_organization)
    return new_organization


def get_organization(db: Session, organization_id: int):
    return (
        db.query(models.Organization)
        .filter(models.Organization.id == organization_id)
        .first()
    )


def get_organizations(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Organization).offset(skip).limit(limit).all()


def get_organization_by_organization_name(db: Session, name: str):
    return (
        db.query(models.Organization).filter(models.Organization.name == name).first()
    )


def get_organization_by_username(db: Session, username: str):
    return (
        db.query(models.Organization)
        .join(models.User)
        .filter(models.User.username == username)
        .first()
    )


def update_organization(
    db: Session, organization_id: int, organization: schemas.OrganizationUpdate
):
    db_organization = (
        db.query(models.Organization)
        .filter(models.Organization.id == organization_id)
        .first()
    )
    if db_organization:
        db_organization.name = organization.name
        db_organization.description = organization.description
        db_organization.target_audience = organization.target_audience
        db_organization.products = organization.products
        db_organization.other_details = organization.other_details
        db.commit()
        db.refresh(db_organization)
        return db_organization
    return None


def delete_organization(db: Session, organization_id: int):
    organization = (
        db.query(models.Organization)
        .filter(models.Organization.id == organization_id)
        .first()
    )
    if organization:
        db.delete(organization)
        db.commit()
        return organization
    return None


# Team CRUD Operations
def create_team(db: Session, team: schemas.TeamCreate):
    new_team = models.Team(
        name=team.name,
        organization_id=team.organization_id,
        campaign_goals=team.campaign_goals,
        selected_products=team.selected_products,
    )
    db.add(new_team)
    db.commit()
    db.refresh(new_team)
    return new_team


def get_team(db: Session, team_id: int):
    return db.query(models.Team).filter(models.Team.id == team_id).first()


def get_teams(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Team).offset(skip).limit(limit).all()


def get_teams_by_organization_id(db: Session, organization_id: int):
    return (
        db.query(models.Team)
        .filter(models.Team.organization_id == organization_id)
        .all()
    )


def get_team_by_team_name_and_organization_id(
    db: Session, team_name: str, organization_id: int
):
    return (
        db.query(models.Team)
        .filter(
            models.Team.name == team_name,
            models.Team.organization_id == organization_id,
        )
        .first()
    )


def update_team(db: Session, team_id: int, team: schemas.TeamUpdate):
    db_team = db.query(models.Team).filter(models.Team.id == team_id).first()
    if db_team:
        db_team.name = team.name
        db_team.organization_id = team.organization_id  # This shouldn't be updated
        db_team.campaign_goals = team.campaign_goals
        db_team.selected_products = team.selected_products
        db.commit()
        db.refresh(db_team)
        return db_team
    return None


def delete_team(db: Session, team_id: int):
    team = db.query(models.Team).filter(models.Team.id == team_id).first()
    if team:
        db.delete(team)
        db.commit()
        return team
    return None


# Agent CRUD Operations
def create_agent(db: Session, agent: schemas.AgentCreate):
    new_agent = models.Agent(
        name=agent.name,
        team_id=agent.team_id,
        agent_function=agent.agent_function,
        forwarding_criteria=agent.forwarding_criteria,
        departments=agent.departments,
        other_department=agent.other_department,
        use_elevenlabs=agent.use_elevenlabs,
        voice_id=agent.voice_id,
    )
    db.add(new_agent)
    db.commit()
    db.refresh(new_agent)
    return new_agent


def get_agent(db: Session, agent_id: int):
    return db.query(models.Agent).filter(models.Agent.id == agent_id).first()


def get_agents(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Agent).offset(skip).limit(limit).all()


def get_agent_by_agent_name(db: Session, agent_name: str):
    return db.query(models.Agent).filter(models.Agent.name == agent_name).first()


def get_agents_by_organization_id(db: Session, organization_id: int):
    agents = (
        db.query(models.Agent)
        .filter(models.Agent.team.has(organization_id=organization_id))
        .all()
    )
    return agents


def get_agents_by_team_id(db: Session, team_id: int):
    return db.query(models.Agent).filter(models.Agent.team_id == team_id).all()


def get_agent_by_agent_name_and_team_id_and_organization_id(
    db: Session, agent_name: str, team_id: int, organization_id: int
):
    return (
        db.query(models.Agent)
        .filter(
            models.Agent.name == agent_name,
            models.Agent.team_id == team_id,
            models.Agent.team.has(organization_id=organization_id),
        )
        .first()
    )


def update_agent(db: Session, agent_id: int, agent: schemas.AgentUpdate):
    db_agent = db.query(models.Agent).filter(models.Agent.id == agent_id).first()
    if db_agent:
        db_agent.name = agent.name
        db_agent.team_id = agent.team_id
        db_agent.agent_function = agent.agent_function
        db_agent.forwarding_criteria = agent.forwarding_criteria
        db_agent.departments = agent.departments
        db_agent.other_department = agent.other_department
        db.commit()
        db.refresh(db_agent)
        return db_agent
    return None


def delete_agent(db: Session, agent_id: int):
    agent = db.query(models.Agent).filter(models.Agent.id == agent_id).first()
    if agent:
        db.delete(agent)
        db.commit()
        return agent
    return None


# Chat History CRUD Operations
def create_chat_history(db: Session, chat_history: schemas.ChatHistoryCreate):
    new_chat_history = models.ChatHistory(
        organization_id=chat_history.organization_id,
        team_id=chat_history.team_id,
        agent_id=chat_history.agent_id,
        chat_data=chat_history.chat_data,
        response_time=chat_history.response_time,
    )
    db.add(new_chat_history)
    db.commit()
    db.refresh(new_chat_history)
    return new_chat_history


def get_chat_history(db: Session, chat_history_id: int):
    return (
        db.query(models.ChatHistory)
        .filter(models.ChatHistory.id == chat_history_id)
        .first()
    )


def get_chat_histories(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.ChatHistory).offset(skip).limit(limit).all()


def get_chat_histories_by_organization_id(
    db: Session, organization_id: int, skip: int = 0, limit: int = 100
):
    return (
        db.query(models.ChatHistory)
        .filter(models.ChatHistory.organization_id == organization_id)
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_chat_histories_by_team_id(
    db: Session, team_id: int, skip: int = 0, limit: int = 100
):
    return (
        db.query(models.ChatHistory)
        .filter(models.ChatHistory.team_id == team_id)
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_chat_histories_by_agent_id(
    db: Session, agent_id: int, skip: int = 0, limit: int = 100
):
    return (
        db.query(models.ChatHistory)
        .filter(models.ChatHistory.agent_id == agent_id)
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_chat_history_by_organization_team_agent(
    db: Session, organization_id: int, team_id: int, agent_id: int
):
    return (
        db.query(models.ChatHistory)
        .filter(
            models.ChatHistory.organization_id == organization_id,
            models.ChatHistory.team_id == team_id,
            models.ChatHistory.agent_id == agent_id,
        )
        .first()
    )


def update_chat_history(
    db: Session, chat_history_id: int, chat_history: schemas.ChatHistoryUpdate
):
    raise NotImplementedError("Chat History cannot be updated.")
    db_chat_history = (
        db.query(models.ChatHistory)
        .filter(models.ChatHistory.id == chat_history_id)
        .first()
    )
    if db_chat_history:
        if chat_history.organization_id is not None:
            db_chat_history.organization_id = chat_history.organization_id
        if chat_history.team_id is not None:
            db_chat_history.team_id = chat_history.team_id
        if chat_history.agent_id is not None:
            db_chat_history.agent_id = chat_history.agent_id
        if chat_history.chat_data is not None:
            db_chat_history.chat_data = chat_history.chat_data
        if chat_history.timestamp is not None:
            db_chat_history.timestamp = chat_history.timestamp
        if chat_history.response_time is not None:
            db_chat_history.response_time = chat_history.response_time
        db.commit()
        db.refresh(db_chat_history)
        return db_chat_history
    return None


def delete_chat_history(db: Session, chat_history_id: int):
    chat_history = (
        db.query(models.ChatHistory)
        .filter(models.ChatHistory.id == chat_history_id)
        .first()
    )
    if chat_history:
        db.delete(chat_history)
        db.commit()
        return chat_history
    return None
