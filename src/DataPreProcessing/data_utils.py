"""Utilities for handling different data formats in the cleaning pipeline."""

from __future__ import annotations
from typing import Any, Dict, Iterator, List, Optional, Union, Sequence, TYPE_CHECKING
from collections.abc import MutableMapping
import json
from pathlib import Path

# Forward references for type checking
if TYPE_CHECKING:
    import pandas as pd
    DataFrame = pd.DataFrame
    Series = pd.Series
else:
    DataFrame = Any
    Series = Any

# Type aliases 
JsonDict = Dict[str, Any]
JsonList = List[JsonDict]

# Module level state for pandas
_pd: Optional[Any] = None

def import_pandas() -> bool:
    """Import pandas lazily, returning True if successful."""
    global _pd
    if _pd is None:
        try:
            import pandas  # type: ignore
            _pd = pandas
            return True
        except ImportError:
            return False
    return True

def ensure_pandas() -> None:
    """Ensure pandas is available for data operations."""
    if not import_pandas():
        raise ImportError(
            "pandas is required for this operation. "
            "Install it with: pip install pandas"
        )

def _convert_to_dict(record: Any) -> JsonDict:
    """Convert a record to a dictionary safely."""
    if isinstance(record, dict):
        return record  # type: ignore
    elif hasattr(record, 'to_dict'):
        result = record.to_dict()  # type: ignore
        if isinstance(result, MutableMapping):
            return dict(result)  # type: ignore
        return result  # type: ignore
    else:
        raise ValueError(f"Cannot convert {type(record)} to dictionary")

def read_csv(
    file_path: Union[str, Path], 
    *,  # Force keyword arguments
    encoding: str = 'utf-8',
    separator: str = ',',
    header: Union[int, Sequence[int], None] = 0
) -> Iterator[JsonDict]:
    """Read records from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        encoding: File encoding to use
        separator: Field delimiter
        header: Row number(s) to use as column names
        
    Yields:
        Dictionary records from the file
    """
    ensure_pandas()
    assert _pd is not None

    df: Any = _pd.read_csv(
        str(file_path),
        encoding=encoding,
        sep=separator,
        header=header
    )
    records: List[JsonDict] = df.to_dict('records')  # type: ignore
    yield from records

def read_parquet(file_path: Union[str, Path]) -> Iterator[JsonDict]:
    """Read records from a Parquet file.
    
    Args:
        file_path: Path to the Parquet file
        
    Yields:
        Dictionary records from the file
    """
    ensure_pandas()
    assert _pd is not None
    
    df: Any = _pd.read_parquet(str(file_path))
    records: List[JsonDict] = df.to_dict('records')  # type: ignore
    yield from records

def read_json(file_path: Union[str, Path]) -> Iterator[JsonDict]:
    """Read records from a JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Yields:
        Dictionary records from the file
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content: Union[List[Any], JsonDict] = json.load(f)
        
    if isinstance(content, list):
        for item in content:
            yield _convert_to_dict(item)
    else:
        yield _convert_to_dict(content)

def read_jsonl(file_path: Union[str, Path]) -> Iterator[JsonDict]:
    """Read records from a JSONL file.
    
    Args:
        file_path: Path to the JSONL file
        
    Yields:
        Dictionary records from the file
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():  # Skip empty lines
                yield json.loads(line)

def write_csv(
    records: Sequence[Union[JsonDict, Any]], 
    file_path: Union[str, Path], 
    *,  # Force keyword arguments
    encoding: str = 'utf-8',
    separator: str = ','
) -> None:
    """Write records to a CSV file.
    
    Args:
        records: Records to write
        file_path: Path to the output file
        encoding: File encoding to use
        separator: Field delimiter
    """
    ensure_pandas()
    assert _pd is not None
    
    data = [_convert_to_dict(record) for record in records]
    df: Any = _pd.DataFrame.from_records(data)
    df.to_csv(str(file_path), index=False, encoding=encoding, sep=separator)

def write_parquet(
    records: Sequence[Union[JsonDict, Any]], 
    file_path: Union[str, Path]
) -> None:
    """Write records to a Parquet file.
    
    Args:
        records: Records to write
        file_path: Path to the output file
    """
    ensure_pandas()
    assert _pd is not None
    
    data = [_convert_to_dict(record) for record in records]
    df: Any = _pd.DataFrame.from_records(data)
    df.to_parquet(str(file_path), index=False)

def write_json(
    records: Sequence[Union[JsonDict, Any]], 
    file_path: Union[str, Path]
) -> None:
    """Write records to a JSON file.
    
    Args:
        records: Records to write
        file_path: Path to the output file
    """
    data = [_convert_to_dict(record) for record in records]
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def write_jsonl(
    records: Sequence[Union[JsonDict, Any]], 
    file_path: Union[str, Path]
) -> None:
    """Write records to a JSONL file.
    
    Args:
        records: Records to write
        file_path: Path to the output file
    """
    with open(file_path, 'w', encoding='utf-8') as f:
        for record in records:
            record_dict = _convert_to_dict(record)
            f.write(json.dumps(record_dict) + '\n')

# Old function names for backward compatibility
read_csv_file = read_csv
read_parquet_file = read_parquet 
read_json_file = read_json
write_csv_file = write_csv
write_parquet_file = write_parquet
write_json_file = write_json