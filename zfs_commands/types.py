"""
Shared types for ZFS command execution
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class CommandResult:
    """Standardized result from command execution"""
    returncode: int
    stdout: str
    stderr: str

    @property
    def success(self) -> bool:
        """Check if command succeeded"""
        return self.returncode == 0

    def raise_for_status(self):
        """Raise exception if command failed"""
        if not self.success:
            raise ZFSCommandError(self.returncode, self.stderr)


class ZFSCommandError(Exception):
    """Exception raised when ZFS command fails"""
    def __init__(self, returncode: int, stderr: str):
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"ZFS command failed (rc={returncode}): {stderr}")
