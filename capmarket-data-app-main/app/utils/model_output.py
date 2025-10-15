import json
import re
import time
from typing import Dict, Any


def response_generator(answer_str: str):
    for word in answer_str.split():
        yield word + " "
        time.sleep(0.01)


def clean_response(model_output: str) -> str:
    return re.sub('\\$', '\\$', model_output).strip()


def is_guardrail_error(error_data: Dict[str, Any]) -> bool:
    try:
        if isinstance(error_data.get('message'), str):
            nested_data = json.loads(error_data['message'])
            return 'output_guardrail' in nested_data
        return 'output_guardrail' in error_data
    except (json.JSONDecodeError, KeyError, TypeError):
        return False


def extract_guardrail_info(error_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if isinstance(error_data.get('message'), str):
            nested_data = json.loads(error_data['message'])
            return nested_data.get('output_guardrail', [{}])[0]
        return error_data.get('output_guardrail', [{}])[0]
    except (json.JSONDecodeError, KeyError, TypeError, IndexError):
        return {}


def get_friendly_message(guardrail_info: Dict[str, Any]) -> str:
    categories = guardrail_info.get('categories', {})
    if categories.get('specialized-advice'):
        return ("I'm unable to provide specific financial advice or recommendations. "
                "I can help you with general market data analysis or factual information instead. "
                "Please rephrase your question to focus on data extraction or general market insights.")
    elif categories.get('hate'):
        return "I can't provide content that contains hate speech or discriminatory language."
    elif categories.get('violence') or categories.get('violent-crimes'):
        return "I can't provide content related to violence or violent activities."
    elif categories.get('sexual-content') or categories.get('sex-crimes'):
        return "I can't provide sexual content or content related to sexual crimes."
    elif categories.get('privacy'):
        return "I can't provide content that may violate privacy guidelines."
    else:
        return ("I'm unable to process this request due to content policy restrictions. "
                "Please try rephrasing your question or ask for general information instead.")
