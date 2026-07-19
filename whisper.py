import os
import sys
import time
import threading
import queue
import numpy as np
import sounddevice as sd
from openai import OpenAI

# Import your Piper mouth class
from openvoicechat.tts.base import BaseMouth
class Mouth_piper(BaseMouth):
    def __init__(self, device='cpu', model_path='models/en_US-ryan-high.onnx',
                 config_path='models/en_US-ryan-high.onnx.json',
                 player=sd):
        import piper
        self.model = piper.PiperVoice.load(model_path=model_path,
                                          config_path=config_path,
                                          use_cuda=True if device == 'cuda' else False)
        super().__init__(sample_rate=self.model.config.sample_rate, player=player)
        
    def run_tts(self, text):
        audio = b''
        for i in self.model.synthesize_stream_raw(text):
            audio += i
        return np.frombuffer(audio, dtype=np.int16)

# Configure OpenAI client
client = OpenAI(api_key=os.getenv("LLM_EC2_KEY"))

# Text buffer and processing queue
text_buffer = ""
audio_queue = queue.Queue()
should_exit = threading.Event()

def stream_gpt_response(prompt, mouth):
    """Stream text from GPT model and process through TTS"""
    global text_buffer
    
    try:
        stream = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
            max_tokens=1000
        )
        
        # Process the stream
        for chunk in stream:
            if chunk.choices[0].delta.content:
                new_text = chunk.choices[0].delta.content
                text_buffer += new_text
                
                # Process sentences when we see end punctuation
                if any(p in new_text for p in ['.', '!', '?', '\n']):
                    sentences = []
                    for sep in ['.', '!', '?']:
                        parts = text_buffer.split(sep)
                        if len(parts) > 1:
                            sentences.extend([p + sep for p in parts[:-1]])
                            text_buffer = parts[-1]
                    
                    # Add complete sentences to the TTS queue
                    for sentence in sentences:
                        if sentence.strip():
                            audio_queue.put(sentence.strip())
            
            # Check if we should exit
            if should_exit.is_set():
                break
        
        # Process any remaining text
        if text_buffer.strip():
            audio_queue.put(text_buffer.strip())
            text_buffer = ""
                
    except Exception as e:
        print(f"Error streaming from GPT: {e}")
        should_exit.set()

def process_tts(mouth):
    """Process text through Piper TTS using the provided mouth object"""
    while not should_exit.is_set():
        try:
            # Get text from queue with timeout to check exit flag periodically
            text = audio_queue.get(timeout=0.5)
            
            if text:
                # Process text through Piper
                audio_data = mouth.run_tts(text)
                
                # Play audio directly using sounddevice
                sd.play(audio_data, mouth.sample_rate)
                sd.wait()  # Wait for audio to finish playing
            
            audio_queue.task_done()
                
        except queue.Empty:
            # Just a timeout, continue
            pass
        except Exception as e:
            print(f"Error in TTS processing: {e}")
            should_exit.set()

def main():
    # Initialize Piper TTS with your configuration
    device = os.environ.get("PIPER_DEVICE", "cpu")
    model_path = os.environ.get("PIPER_MODEL_PATH", "models/en_US-ryan-high.onnx")
    config_path = os.environ.get("PIPER_CONFIG_PATH", "models/en_US-ryan-high.onnx.json")
    
    # Create TTS engine
    print(f"Initializing Piper with model: {model_path}")
    mouth = Mouth_piper(device=device, model_path=model_path, config_path=config_path)
    
    # Start TTS processing thread
    tts_thread = threading.Thread(target=process_tts, args=(mouth,))
    tts_thread.daemon = True
    tts_thread.start()
    
    try:
        while True:
            prompt = input("\nEnter prompt for GPT (or 'exit' to quit): ")
            if prompt.lower() == "exit":
                break
                
            # Start streaming in a separate thread
            gpt_thread = threading.Thread(target=stream_gpt_response, args=(prompt, mouth))
            gpt_thread.start()
            
            print("Streaming response (press Ctrl+C to stop)...")
            
            # Wait for completion or interrupt
            gpt_thread.join()
            
            # Wait for TTS queue to empty
            audio_queue.join()
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        # Signal all threads to exit
        should_exit.set()
        
if __name__ == "__main__":
    main()