from typing import Any, Dict, Union

from .cleaningStep import CleaningStep, Record


class QualityFilter(CleaningStep):
    """Filter records based on length constraints.
    
    This filter helps remove inputs that are too small (which could introduce bias)
    or too large (which could be noisy or break memory constraints during training).
    """
    
    def __init__(self, min_length: float = 0, max_length: float = float('inf')):
        """Initialize the quality filter.
        
        Args:
            min_length: Minimum allowed length (inclusive)
            max_length: Maximum allowed length (inclusive)
        """
        self.min_length = min_length
        self.max_length = max_length
    
    def filter(self, record: Record) -> bool:
        """Check if a record passes the length constraints.
        
        Args:
            record: The record to check
            
        Returns:
            True if the record passes the filter, False if it should be filtered out
        """
        text = self.get_text(record)
        length = len(text.split()) if text else 0  # Word count
        return self.min_length <= length <= self.max_length
    
    def process(self, data: Record) -> Union[Record, None]:
        """Process a single record, filtering based on length.
        
        Args:
            data: The record to process
            
        Returns:
            The record if it passes the filter, None if it should be filtered out
        """
        return data if self.filter(data) else None
    
    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration.
        
        Returns:
            Dictionary with min and max length settings
        """
        return {
            "min_length": self.min_length,
            "max_length": self.max_length
        }
