from typing import Union, Tuple, List
from base64 import b64encode 
from datetime import datetime
from icecream import ic

def response_to_dict(response: Union[Tuple[str], List[str]], keys: Tuple[str]):
    return list(map(lambda items: dict(zip(keys, items)), response))

def serialize_response(response: Union[Tuple[str], List[str]], keys: Tuple[str]):
    response = tuple(b64encode(item.tobytes()).decode('ascii') if isinstance(item, memoryview) else item for item in response[0])
    return response_to_dict((response, ), keys)

def validate_datetime_from_form(form, key: str):
    date_ = form.get(key)
    form[key] = datetime.today() if not date_ else datetime.strptime(date_, '%d/%m/%Y')
    return form