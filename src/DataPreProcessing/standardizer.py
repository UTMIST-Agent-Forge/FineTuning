import re
from typing import Any, Dict, Union

from cleaningStep import CleaningStep, Record


class Standardizer(CleaningStep):
    """Standardize text data through various normalization steps."""
    
    def __init__(
        self,
        trim_whitespace: bool = True,
        normalize_whitespace: bool = True,
        standardize_punctuation: bool = True
    ):
        """Initialize the standardizer.
        
        Args:
            trim_whitespace: Whether to trim leading/trailing whitespace
            normalize_whitespace: Whether to normalize internal whitespace
            standardize_punctuation: Whether to standardize punctuation
        """
        self.trim_whitespace = trim_whitespace
        self.normalize_whitespace = normalize_whitespace
        self.standardize_punctuation = standardize_punctuation
    
    def transform(self, record: Record) -> Record:
        """Apply text standardization to a record.
        
        Args:
            record: The record to transform
            
        Returns:
            The transformed record
        """
        # Get the text and convert to lowercase
        text = self.get_text(record).lower()
        record = record.copy()  # Don't modify original

        # Standardize structure - ensure metadata is in proper format
        metadata = {}
        if 'metadata' in record:
            metadata = record['metadata'].copy()
        
        # Move top-level metadata fields into metadata dict if they exist
        for field in ['source', 'date']:
            if field in record:
                metadata[field] = record.pop(field)
            
        if self.trim_whitespace:
            text = text.strip()
            
        if self.normalize_whitespace:
            text = re.sub(r'\s+', ' ', text)
            
        if self.standardize_punctuation:
            # Standardize quotes (convert all quotes to basic ASCII quotes)
            quotes_map = {
                '"': '"',  # U+201C LEFT DOUBLE QUOTATION MARK
                '"': '"',  # U+201D RIGHT DOUBLE QUOTATION MARK
                ''': "'",  # U+2018 LEFT SINGLE QUOTATION MARK
                ''': "'",  # U+2019 RIGHT SINGLE QUOTATION MARK
            }
            for smart, straight in quotes_map.items():
                text = text.replace(smart, straight)
            
            # Add space after punctuation if missing (but not multiple spaces)
            for punct in '.!?':
                text = text.replace(f'{punct}', f'{punct} ')
            text = ' '.join(text.split())  # Normalize spaces
            
            # Remove multiple consecutive punctuation marks
            prev_text = ''
            while prev_text != text:
                prev_text = text
                for punct in '.!?':
                    text = text.replace(f'{punct}{punct}', punct)
            
        # Update record with standardized text
        record = self.set_field(record, 'text', text)
        
        # Add metadata structure if any metadata fields exist
        if metadata:
            record['metadata'] = metadata
            
        return record

    def process(self, data: Record) -> Union[Record, None]:
        """Process a single record by standardizing its text.
        
        Args:
            data: The record to process
            
        Returns:
            The standardized record
        """
        return self.transform(data)

    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration.
        
        Returns:
            Dictionary with standardization settings
        """
        return {
            "trim_whitespace": self.trim_whitespace,
            "normalize_whitespace": self.normalize_whitespace,
            "standardize_punctuation": self.standardize_punctuation
        }
