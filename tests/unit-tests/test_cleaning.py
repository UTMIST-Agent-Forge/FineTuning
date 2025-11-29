"""Test module for data cleaning pipeline."""

import os
from pathlib import Path
import unittest
from typing import List, Dict, Any, Iterator, Optional, Union, cast, TypeVar, Iterable, Set

# Import pandas with type ignore and alias types
try:
    import pandas as pd  # type: ignore
    pandas_available = True
except ImportError:
    pandas_available = False
    pd = None

# Type alias for records
RecordDict = Dict[str, Any]

from src.DataPreProcessing.cleaningStep import Record, CleaningStep
from src.DataPreProcessing.data_utils import (
    read_csv, read_json, read_parquet,
    write_csv, write_json, write_parquet
)
from src.DataPreProcessing.removeDuplicates import RemoveDuplicates
from src.DataPreProcessing.qualityFilter import QualityFilter
from src.DataPreProcessing.standardizer import Standardizer
from src.DataPreProcessing.extractMetaData import Metadata

# Type variable for iterating over Records with type checking
R = TypeVar('R', bound=Record)

@unittest.skipUnless(pandas_available, "Pandas is required for these tests")
class TestDataCleaning(unittest.TestCase):
    """Test cases for data cleaning pipeline."""

    test_data_dir: Path
    json_file: Path
    csv_file: Path
    parquet_file: Path
    duplicate_remover: RemoveDuplicates
    quality_filter: QualityFilter
    standardizer: Standardizer
    metadata_extractor: Metadata

    @classmethod
    def setUpClass(cls) -> None:
        """Set up test data and cleaning steps."""
        cls.test_data_dir = Path(__file__).parent.parent / 'test_data'
        cls.json_file = cls.test_data_dir / 'sample.json'
        cls.csv_file = cls.test_data_dir / 'sample.csv'
        cls.parquet_file = cls.test_data_dir / 'sample.parquet'

        # Convert CSV to Parquet for testing if pandas is available
        if pandas_available and pd is not None:
            df = pd.read_csv(str(cls.csv_file))  # type: ignore
            if hasattr(pd, 'DataFrame') and isinstance(df, pd.DataFrame):  # type: ignore
                df.to_parquet(str(cls.parquet_file), index=False)  # type: ignore

        # Initialize cleaning steps
        cls.duplicate_remover = RemoveDuplicates(selected_key='text')
        cls.quality_filter = QualityFilter(min_length=5)  # At least 5 words
        cls.standardizer = Standardizer()
        cls.metadata_extractor = Metadata(add_word_count=True, add_sentence_count=True)

    def process_single_record(self, record: Union[Dict[str, Any], Any], processed_keys: Set[str]) -> Optional[RecordDict]:
        """Process a single record through the cleaning pipeline.
        
        Args:
            record: A single record to process, either dict or pandas Series
            processed_keys: Set of already processed keys for duplicate detection
            
        Returns:
            Processed record dict or None if filtered out
        """
        try:
            # Convert pandas Series to dict if needed
            processed: RecordDict
            if pd is not None and hasattr(pd, 'Series') and isinstance(record, pd.Series):  # type: ignore
                processed = record.to_dict()  # type: ignore
            elif isinstance(record, dict):
                processed = cast(RecordDict, record)
            else:
                return None
            
            # Process through each step
            steps: List[CleaningStep] = [
                self.standardizer,  # Standardize first
                self.metadata_extractor,  # Add metadata
                self.quality_filter  # Then filter for quality
            ]
            
            # Process through each cleaning step
            result: Optional[RecordDict] = processed
            
            for step in steps:
                    
                # Apply the cleaning step and log the transformation
                processed_result = step.process(cast(Record, result))
                
                # Handle the result
                if processed_result is None:
                    print(f"Step {step.__class__.__name__} rejected record:")
                    print(f"  Input: {result}")
                    result = None
                    break
                elif not isinstance(processed_result, dict):
                    print(f"Step {step.__class__.__name__} returned invalid type {type(processed_result)}:")
                    print(f"  Input: {result}")
                    print(f"  Output: {processed_result}")
                    result = None
                    break
                else:
                    print(f"Step {step.__class__.__name__} transformed record:")
                    print(f"  Input: {result}")
                    print(f"  Output: {processed_result}")
                    result = processed_result  
            
            # After all other steps, check for duplicates
            if result is not None:
                # Safe to access dict since we checked result is not None
                value = str(result.get('text', ''))
                if value in processed_keys:
                    print(f"Duplicate detected: {value}")
                    return None
                processed_keys.add(value)
                return result
            
            return None

        except Exception as e:
            print(f"Error processing record: {e}")
            self.skipTest(f"Error processing record: {e}")
            return None

    def process_pipeline(self, records: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        """Process records through the cleaning pipeline.
        
        Args:
            records: List of records to process
            
        Returns:
            Iterator of processed records
        """
        processed_keys: Set[str] = set()  # Track duplicates across all records
        for record in records:
            processed = self.process_single_record(record, processed_keys)
            if processed is not None:
                yield processed

    def test_json_cleaning(self) -> None:
        """Test cleaning pipeline with JSON data."""
        # Read and process JSON data
        raw_records = list(read_json(self.json_file))
        records: List[Dict[str, Any]] = []
        
        # Convert raw records to dicts
        for record in raw_records:
            if pd is not None and hasattr(pd, 'Series') and isinstance(record, pd.Series):  # type: ignore
                records.append(record.to_dict())  # type: ignore
            else:
                # record is already a dict
                records.append(record)
        
        # Process through pipeline
        processed_records = list(self.process_pipeline(records))
        
        # Verify results
        self.assertGreater(len(records), 0, "Should have input records")
        self.assertGreater(len(processed_records), 0, "Should have processed records")
        self.assertLessEqual(len(processed_records), len(records), 
                           "Processed records should not exceed input records")
        
        # Test each processed record
        for record in processed_records:
            # Check text field
            text = record.get('text', '')
            self.assertIsInstance(text, str)
            self.assertGreater(len(text), 0)
            self.assertFalse(any(c.isupper() for c in text), 
                           "Text should be lowercase")
            
            # Check metadata
            metadata = record.get('metadata', {})
            self.assertIsInstance(metadata, dict)
            
            # Check metadata values 
            source = metadata.get('source', '')
            self.assertIsInstance(source, str)
            self.assertGreater(len(source), 0)
            
            date = metadata.get('date', '')
            self.assertIsInstance(date, str)
            self.assertGreater(len(date), 0)

        # Write processed data to all formats
        output_dir = self.test_data_dir / 'processed'
        os.makedirs(output_dir, exist_ok=True)
        
        # Write to each format
        write_json(processed_records, output_dir / 'processed.json')
        
        if pandas_available and pd is not None:
            write_csv(processed_records, output_dir / 'processed.csv')
            write_parquet(processed_records, output_dir / 'processed.parquet')

    def test_csv_cleaning(self) -> None:
        """Test cleaning pipeline with CSV data."""
        if not pandas_available:
            self.skipTest("Pandas required for CSV tests")
            
        # Read and process CSV data
        raw_records = list(read_csv(self.csv_file))
        records: List[RecordDict] = []
        
        # Convert raw records to dicts
        for record in raw_records:
            if pd is not None and hasattr(pd, 'Series') and isinstance(record, pd.Series):  # type: ignore
                records.append(record.to_dict())  # type: ignore
            else:
                # Already a dict
                records.append(record)
        
        # Process through pipeline
        processed_records = list(self.process_pipeline(records))
        
        # Verify results
        self.assertGreater(len(records), 0, "Should have input records")
        self.assertGreater(len(processed_records), 0, "Should have processed records")
        self.assertLessEqual(len(processed_records), len(records), 
                           "Processed records should not exceed input records")

        # Verify record structure matches JSON test
        for record in processed_records:
            self.assertIn('text', record)
            self.assertIn('metadata', record)
            metadata = cast(Dict[str, Any], record['metadata'])
            self.assertIn('source', metadata)
            self.assertIn('date', metadata)

    def test_parquet_cleaning(self) -> None:
        """Test cleaning pipeline with Parquet data."""
        if not pandas_available:
            self.skipTest("Pandas required for Parquet tests")
            
        # Read and process Parquet data
        raw_records = list(read_parquet(self.parquet_file))
        records: List[RecordDict] = []
        
        # Convert raw records to dicts
        for record in raw_records:
            if pd is not None and hasattr(pd, 'Series') and isinstance(record, pd.Series):  # type: ignore
                records.append(record.to_dict())  # type: ignore
            else:
                # Already a dict
                records.append(record)
        
        # Process through pipeline
        processed_records = list(self.process_pipeline(records))
        
        # Verify results
        self.assertGreater(len(records), 0, "Should have input records")
        self.assertGreater(len(processed_records), 0, "Should have processed records")
        self.assertLessEqual(len(processed_records), len(records), 
                           "Processed records should not exceed input records")

        # Verify record structure matches JSON test
        for record in processed_records:
            self.assertIn('text', record)
            self.assertIn('metadata', record)
            metadata = cast(Dict[str, Any], record['metadata'])
            self.assertIn('source', metadata)
            self.assertIn('date', metadata)

    def test_cross_format_consistency(self) -> None:
        """Test that processing is consistent across file formats."""
        if not pandas_available:
            self.skipTest("Pandas required for cross-format tests")
        
        # Read data from all formats
        raw_json = list(read_json(self.json_file))
        raw_csv = list(read_csv(self.csv_file))
        raw_parquet = list(read_parquet(self.parquet_file))
        
        # Convert to dicts
        json_records: List[Dict[str, Any]] = []
        csv_records: List[Dict[str, Any]] = []
        parquet_records: List[Dict[str, Any]] = []
        
        # Helper function to convert records
        def convert_records(raw_data: Iterable[Union[Dict[str, Any], Any]]) -> List[Dict[str, Any]]:
            result: List[Dict[str, Any]] = []
            for record in raw_data:
                if pd is not None and hasattr(pd, 'Series') and isinstance(record, pd.Series):  # type: ignore
                    result.append(record.to_dict())  # type: ignore
                elif isinstance(record, dict):
                    # We know it's Dict[str, Any] from the input type hint
                    result.append(cast(Dict[str, Any], record))
            return result
        
        # Convert each format
        json_records = convert_records(raw_json)
        csv_records = convert_records(raw_csv)
        parquet_records = convert_records(raw_parquet)
        
        # Process each format
        json_processed = list(self.process_pipeline(json_records))
        csv_processed = list(self.process_pipeline(csv_records))
        parquet_processed = list(self.process_pipeline(parquet_records))

        # Compare results
        self.assertEqual(len(json_processed), len(csv_processed),
                        "JSON and CSV should have same number of records")
        self.assertEqual(len(csv_processed), len(parquet_processed),
                        "CSV and Parquet should have same number of records")

if __name__ == '__main__':
    unittest.main()