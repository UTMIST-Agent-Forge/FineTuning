
from abc import ABC, abstractmethod
from typing import Any, Dict, Union
import pandas as pd

# Type alias for flexible record types
Record = Union[Dict[str, Any], pd.Series]

class CleaningStep(ABC):
    """Abstract base class for data cleaning steps.
    
    This class defines the interface for all cleaning steps in the pipeline.
    Each step can either transform the data or filter it based on criteria.
    """
    
    def get_text(self, record: Record, default: str = '') -> str:
        """Safely get text from either dict or Series record.
        
        Args:
            record: The record to access
            default: Default value if text field is missing
            
        Returns:
            The text content or default value
        """
        if isinstance(record, dict):
            return record.get('text', default)
        # For pandas Series, ensure we get a string
        if 'text' in record.index:
            return str(record['text'])
        return default
    
    def get_field(self, record: Record, field: str, default: Any = None) -> Any:
        """Safely get any field from either dict or Series record.
        
        Args:
            record: The record to access
            field: The field name to access
            default: Default value if field is missing
            
        Returns:
            The field value or default
        """
        try:
            if isinstance(record, dict):
                return record.get(field, default)
            # For pandas Series
            if field in record.index:
                value = record[field]
                # Convert numpy/pandas types to Python native types
                return value.item() if hasattr(value, 'item') else value
            return default
        except (KeyError, AttributeError):
            return default
    
    def set_field(self, record: Record, field: str, value: Any) -> Record:
        """Safely set a field in either dict or Series record.
        
        Args:
            record: The record to modify
            field: The field name to set
            value: The value to set
            
        Returns:
            A new record with the field set
        """
        if isinstance(record, dict):
            new_record = record.copy()
            new_record[field] = value
            return new_record
        new_record = record.copy()
        new_record[field] = value
        return new_record
    
    @abstractmethod
    def process(self, data: Record) -> Union[Record, None]:
        """Process a single record, either transforming or filtering it.
        
        Args:
            data: The record to process, either as a dict or pandas Series
            
        Returns:
            The processed record, or None if the record should be filtered out
        """
        pass
    
    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration of this cleaning step.
        
        Returns:
            A dictionary containing the current configuration
        """
        pass