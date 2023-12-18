from dataclasses import dataclass

@dataclass
class Lawyer:
    """Class for holding lawyer information instead of a dict"""
    name: str
    status: str
    call_date: str
    primary_location: str
    address: str
    phone: str
    fax: str


