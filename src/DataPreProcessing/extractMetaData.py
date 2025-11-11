from typing import Any, Dict, Union

from .cleaningStep import CleaningStep, Record


class Metadata(CleaningStep):
    """Add metadata like word and sentence counts to records."""

    def __init__(self, add_word_count: bool = False, add_sentence_count: bool = False):
        """Initialize the Metadata cleaning step.

        Args:
            add_word_count: Whether to compute and add word counts
            add_sentence_count: Whether to compute and add sentence counts
        """
        self.add_word_count = add_word_count
        self.add_sentence_count = add_sentence_count


    def get_sentence_count(self, record: Record) -> int:
        """Compute the sentence count for text in the record.
        
        Args:
            record: The record containing text to analyze

        Returns:
            sentence count for text in the record
        """
        text = self.get_text(record)
        if not text:
            sentence_count = 0
            return sentence_count

        endings = [".", "!", "?"]
        count = 0
        for char in text:
            if char in endings:
                count += 1

        sentence_count = max(1, count)  # At least 1 sentence if there's text
        return sentence_count

    def get_word_count(self, record: Record) -> int:
        """Compute the word count for text in the record.
        
        Args:
            record: The record containing text to analyze

        Returns:
            word count for text in the record
        """
        text = self.get_text(record)
        word_count = len(text.split()) if text else 0
        return word_count

    def process(self, data: Record) -> Union[Record, None]:
        """Add configured metadata fields to the record.
        
        Args:
            data: The record to analyze and augment with metadata

        Returns:
            The record with added metadata fields
        """
        record = data.copy()  # Always copy to avoid modifying original
            
        if self.add_word_count:
            word_count = self.get_word_count(record)
            record = self.set_field(record, 'word_count', word_count)

        if self.add_sentence_count:
            sentence_count = self.get_sentence_count(record)
            record = self.set_field(record, 'sentence_count', sentence_count)
            
        return record

    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration of this cleaning step.
        
        Returns:
            A dictionary containing the current configuration
        """
        return {
            "add_word_count": self.add_word_count,
            "add_sentence_count": self.add_sentence_count,
        }
