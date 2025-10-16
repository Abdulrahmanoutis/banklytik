import requests
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

def call_deepseek(prompt_data: dict, timeout: int = 60) -> str:
    """
    Call DeepSeek API with the given prompt data
    """
    try:
        # Get API configuration from settings
        api_key = getattr(settings, 'DEEPSEEK_API_KEY', 'your-api-key-here')
        api_url = getattr(settings, 'DEEPSEEK_API_URL', 'https://api.deepseek.com/chat/completions')
        
        if not api_key or api_key == 'your-api-key-here':
            raise ValueError("DeepSeek API key not configured in settings")
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }
        
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": prompt_data["system"]},
                {"role": "user", "content": prompt_data["user"]}
            ],
            "temperature": 0.1,
            "max_tokens": 4000,
            "stream": False  # Ensure we don't use streaming
        }
        
        print(f"DEBUG: Calling DeepSeek API with timeout {timeout}s")
        print(f"DEBUG: Payload size: {len(str(payload))} characters")
        
        response = requests.post(api_url, json=payload, headers=headers, timeout=timeout)
        
        # Check for HTTP errors
        if response.status_code != 200:
            raise Exception(f"DeepSeek API returned status {response.status_code}: {response.text}")
        
        result = response.json()
        
        if 'choices' not in result or len(result['choices']) == 0:
            raise Exception("DeepSeek API returned no choices")
        
        content = result['choices'][0]['message']['content']
        print(f"DEBUG: DeepSeek response received: {len(content)} characters")
        
        return content
        
    except requests.exceptions.Timeout:
        raise Exception(f"DeepSeek API timeout after {timeout} seconds")
    except requests.exceptions.ConnectionError:
        raise Exception("Failed to connect to DeepSeek API")
    except Exception as e:
        logger.error(f"DeepSeek API call failed: {str(e)}")
        raise