"""
Unit tests for UtilsSTD class
"""
import pytest
from datetime import datetime
from src.utils.utils_std import UtilsSTD

def test_get_now_str():
    """Test get_now_str method"""
    result = UtilsSTD.get_now_str()
    assert isinstance(result, str)
    # Verify format
    datetime.strptime(result, '%Y-%m-%d %H:%M:%S')

def test_get_now_datetime():
    """Test get_now_datetime method"""
    result = UtilsSTD.get_now_datetime()
    assert isinstance(result, datetime)

@pytest.mark.parametrize("suffix,expected_count", [
    (".py", 2),
    (".txt", 0),
    ("", 3)
])
def test_get_filenames(tmp_path, suffix, expected_count):
    """Test get_filenames method with different suffixes"""
    # Create test files
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    (test_dir / "file1.py").write_text("")
    (test_dir / "file2.py").write_text("")
    (test_dir / "file3.csv").write_text("")
    
    result = UtilsSTD.get_filenames(str(test_dir), suffix=suffix)
    assert isinstance(result, list)
    assert len(result) == expected_count
    if suffix:
        assert all(f.endswith(suffix) for f in result)

def test_get_filenames_with_subdirs(tmp_path):
    """Test get_filenames with subdirectories"""
    # Create test directory structure
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()
    (test_dir / "subdir").mkdir()
    (test_dir / "file1.py").write_text("")
    (test_dir / "subdir" / "file2.py").write_text("")
    
    result = UtilsSTD.get_filenames(str(test_dir), suffix=".py")
    assert len(result) == 1