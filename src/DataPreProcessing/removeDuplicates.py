from typing import Any, Dict, List, Set, Union

from .cleaningStep import CleaningStep, Record


class RemoveDuplicates(CleaningStep):
    """Remove duplicate records based on a selected key."""
    
    def __init__(self, selected_key: str):
        """Initialize the duplicate removal step.
        
        Args:
            selected_key: The key in the record to use for duplicate detection
        """
        self.selected_key = selected_key
        self._seen_values: Set[str] = set()
        
    def detect_duplicate(self, item: Record) -> bool:
        """Check if a record is a duplicate based on the selected key.
        
        Args:
            item: The record to check
            
        Returns:
            True if the item is a duplicate, False otherwise
        """
        value = str(self.get_field(item, self.selected_key))
            
        if value in self._seen_values:
            return True
            
        self._seen_values.add(value)
        return False
    
    def remove_duplicates(self, data: List[Record]) -> List[Record]:
        """Remove duplicate records from a list of records.
        
        Args:
            data: List of records to deduplicate
            
        Returns:
            List of unique records
        """
        self._seen_values.clear()  # Reset for fresh batch
        return [item for item in data if not self.detect_duplicate(item)]
    
    def process(self, data: Record) -> Union[Record, None]:
        """Process a single record, filtering out duplicates.
        
        Args:
            data: The record to process
            
        Returns:
            The record if it's not a duplicate, None if it is
        """
        return None if self.detect_duplicate(data) else data
    
    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration.
        
        Returns:
            Dictionary with the selected key and number of seen values
        """
        return {
            "selected_key": self.selected_key,
            "seen_values_count": len(self._seen_values)
        }
