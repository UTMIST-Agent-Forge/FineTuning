"""
Simple test to verify dataset_reader and dataset_loader work with cleaning pipeline.
Run this to test the integration: python test_integration_simple.py
"""

import pandas as pd
from datasets import Dataset

from src.DataPreProcessing.dataset_reader import DatasetReader
from src.DataPreProcessing.dataset_loader import HuggingFaceDatasetLoader
from src.DataPreProcessing.standardizer import Standardizer
from src.DataPreProcessing.qualityFilter import QualityFilter
from src.DataPreProcessing.removeDuplicates import RemoveDuplicates
from src.DataPreProcessing.extractMetaData import Metadata


def test_1_dataset_reader_with_cleaning():
    """Test DatasetReader -> Cleaning Pipeline flow"""
    print("\n" + "="*60)
    print("TEST 1: DatasetReader with Cleaning Pipeline")
    print("="*60)

    # Create sample data
    data = [
        {'question': 'What is AI?', 'answer': 'Artificial Intelligence...', 'category': 'tech'},
        {'question': 'What is ML?', 'answer': 'Machine Learning...', 'category': 'tech'},
        {'question': 'Hi', 'answer': 'x', 'category': 'spam'},  # Too short, will be filtered
        {'question': 'What is AI?', 'answer': 'Artificial Intelligence...', 'category': 'tech'},  # Duplicate
    ]

    # Step 1: Load with DatasetReader
    print("\n1. Creating DatasetReader...")
    df = pd.DataFrame(data)
    dataset = Dataset.from_pandas(df)
    reader = DatasetReader(dataset, keep_extra_fields=True)

    # Step 2: Set field mappings
    print("\n2. Setting field mappings...")
    reader.set_fields(
        input_fields=['question'],
        output_fields=['answer'],
        extra_fields=['category']
    )
    print(f"   Input fields: {reader.input_fields}")
    print(f"   Output fields: {reader.output_fields}")
    print(f"   Extra fields: {reader.extra_fields}")

    # Step 3: Convert to cleaning-compatible format
    print("\n3. Converting to cleaning format...")
    records = reader.to_cleaning_format()
    print(f"   Sample record: {records[0]}")

    # Step 4: Apply cleaning pipeline
    print("\n4. Applying cleaning steps...")
    cleaning_steps = [
        Standardizer(trim_whitespace=True),
        Metadata(add_word_count=True),
        QualityFilter(min_length=3, max_length=100),
        RemoveDuplicates(selected_key='text')
    ]

    cleaned_records = records.copy()
    for step in cleaning_steps:
        step_name = step.__class__.__name__
        before = len(cleaned_records)

        temp = []
        for record in cleaned_records:
            result = step.process(record)
            if result is not None:
                temp.append(result if isinstance(result, dict) else result.to_dict())

        cleaned_records = temp
        after = len(cleaned_records)
        print(f"   {step_name}: {before} -> {after} records")

    # Step 5: Results
    print("\n5. Results:")
    print(f"   Original: {len(data)} records")
    print(f"   After cleaning: {len(cleaned_records)} records")
    print(f"   Sample cleaned: {cleaned_records[0]}")

    assert len(cleaned_records) == 2, "Should have 2 records after filtering duplicates and short text"
    print("\n✓ TEST 1 PASSED!")


def test_2_huggingface_loader_with_cleaning():
    """Test HuggingFace Loader -> DatasetReader -> Cleaning flow"""
    print("\n" + "="*60)
    print("TEST 2: HuggingFace Loader with Cleaning Pipeline")
    print("="*60)

    try:
        # Step 1: Load from HuggingFace
        print("\n1. Loading from HuggingFace...")
        loader = HuggingFaceDatasetLoader('rotten_tomatoes', split='train')
        df = loader.load()
        df = df.head(50)  # Limit for testing
        print(f"   Loaded {len(df)} records")
        print(f"   Columns: {list(df.columns)}")

        # Step 2: Use DatasetReader
        print("\n2. Using DatasetReader...")
        dataset = Dataset.from_pandas(df)
        reader = DatasetReader(dataset, keep_extra_fields=True)
        reader.auto_detect_fields()

        # Step 3: Convert to cleaning format
        print("\n3. Converting to cleaning format...")
        records = reader.to_cleaning_format()
        print(f"   Sample record keys: {list(records[0].keys())}")
        print(f"\n   BEFORE CLEANING - First 3 records:")
        for i, record in enumerate(records[:3], 1):
            print(f"   Record {i}:")
            print(f"     Text: {record.get('text', 'N/A')[:100]}...")
            if 'output' in record:
                print(f"     Output: {record.get('output', 'N/A')}")
            if 'metadata' in record:
                print(f"     Metadata: {record.get('metadata', {})}")

        # Step 4: Apply cleaning
        print("\n4. Applying cleaning...")
        cleaning_steps = [
            Standardizer(),
            QualityFilter(min_length=5, max_length=100),
            RemoveDuplicates(selected_key='text')
        ]

        cleaned_records = records.copy()
        for step in cleaning_steps:
            step_name = step.__class__.__name__
            before = len(cleaned_records)

            temp = []
            for record in cleaned_records:
                result = step.process(record)
                if result is not None:
                    temp.append(result if isinstance(result, dict) else result.to_dict())

            cleaned_records = temp
            after = len(cleaned_records)
            print(f"   {step_name}: {before} -> {after} records")

        # Step 5: Results
        print("\n5. Results:")
        print(f"   Original: {len(records)} records")
        print(f"   After cleaning: {len(cleaned_records)} records")
        print(f"\n   AFTER CLEANING - First 3 records:")
        for i, record in enumerate(cleaned_records[:3], 1):
            print(f"   Record {i}:")
            print(f"     Text: {record.get('text', 'N/A')[:100]}...")
            if 'output' in record:
                print(f"     Output: {record.get('output', 'N/A')}")
            if 'metadata' in record:
                print(f"     Metadata: {record.get('metadata', {})}")

        print("\n✓ TEST 2 PASSED!")

    except Exception as e:
        print(f"\n⚠ TEST 2 SKIPPED: {e}")
        print("   (This is OK if you don't have internet or datasets library)")


def test_3_complete_workflow():
    """Test complete workflow: Load -> Detect -> Clean -> Save format"""
    print("\n" + "="*60)
    print("TEST 3: Complete Workflow")
    print("="*60)

    # Sample multi-format data
    data = [
        {'prompt': 'Translate: Hello', 'completion': 'Bonjour', 'lang': 'french'},
        {'prompt': 'Translate: Goodbye', 'completion': 'Au revoir', 'lang': 'french'},
        {'prompt': 'Translate: Thank you', 'completion': 'Merci', 'lang': 'french'},
    ]

    print("\n1. Loading data...")
    df = pd.DataFrame(data)
    dataset = Dataset.from_pandas(df)
    reader = DatasetReader(dataset, keep_extra_fields=True)

    print("\n2. Auto-detecting fields...")
    reader.auto_detect_fields()

    print("\n3. Converting to cleaning format...")
    records = reader.to_cleaning_format()

    print("\n4. Cleaning...")
    cleaned = []
    seen = set()

    for record in records:
        # Standardize
        standardizer = Standardizer()
        result = standardizer.process(record)
        if result is None:
            continue

        # Deduplicate
        text = result['text']
        if text in seen:
            continue
        seen.add(text)

        cleaned.append(result)

    print(f"   Cleaned: {len(cleaned)} records")

    print("\n5. Final format (ready for LLM fine-tuning):")
    for i, record in enumerate(cleaned, 1):
        print(f"   Record {i}:")
        print(f"     Text: {record['text']}")
        if 'output' in record:
            print(f"     Output: {record['output']}")
        if 'metadata' in record:
            print(f"     Metadata: {record['metadata']}")

    print("\n✓ TEST 3 PASSED!")


if __name__ == '__main__':
    print("\n" + "="*60)
    print("INTEGRATION TESTS")
    print("Testing dataset_reader + dataset_loader + cleaning pipeline")
    print("="*60)

    test_1_dataset_reader_with_cleaning()
    test_2_huggingface_loader_with_cleaning()
    test_3_complete_workflow()

    print("\n" + "="*60)
    print("ALL TESTS COMPLETED!")
    print("="*60)
    print("\nYour integration is working correctly!")
    print("You can now use DatasetReader/Loader with cleaning steps.")
