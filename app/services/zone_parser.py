import gzip
from pathlib import Path
from typing import List, Dict, Any, Optional, Generator, Tuple
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50000


@dataclass
class DomainRecord:
    """Represents a domain with its DNS records."""
    domain: str
    ns: List[str] = field(default_factory=list)
    a: List[str] = field(default_factory=list)
    aaaa: List[str] = field(default_factory=list)
    ds: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB."""
        result = {"domain": self.domain}
        if self.ns:
            result["ns"] = self.ns
        if self.a:
            result["a"] = self.a
        if self.aaaa:
            result["aaaa"] = self.aaaa
        if self.ds:
            result["ds"] = self.ds
        return result


class ZoneParser:
    """
    Parser for BIND zone files from ICANN CZDS.
    
    OPTIMIZED: Uses chunked parsing to avoid OOM on large files (1M+ domains).
    """
    
    RECORD_TYPES = {'ns', 'a', 'aaaa', 'ds'}
    
    def __init__(self, file_path: Path, chunk_size: int = CHUNK_SIZE):
        self.file_path = file_path
        self.tld = self._extract_tld()
        self.chunk_size = chunk_size
    
    def _extract_tld(self) -> str:
        """Extract TLD from filename."""
        name = self.file_path.name
        tld = name.replace(".txt.gz", "").replace(".zone.gz", "").replace(".gz", "").replace(".txt", "")
        return tld
    
    def _read_file(self):
        """Read file line by line, handling gzip compression."""
        try:
            if self.file_path.suffix == ".gz":
                with gzip.open(self.file_path, "rt", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        yield line
            else:
                with open(self.file_path, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        yield line
        except Exception as e:
            logger.error(f"Error reading file {self.file_path}: {str(e)}")
            raise
    
    def parse_domains_chunked(self) -> Generator[Tuple[Dict[str, DomainRecord], bool], None, None]:
        """
        Parse zone file and yield chunks of domains.
        
        MEMORY OPTIMIZED: Yields chunks of chunk_size domains instead of loading all at once.
        
        Yields:
            Tuple of (domains_dict, is_last_chunk)
        """
        domains: Dict[str, DomainRecord] = {}
        tld_suffix = f".{self.tld}."
        tld_suffix_lower = tld_suffix.lower()
        
        line_count = 0
        total_domains = 0
        
        for line in self._read_file():
            line_count += 1
            
            if line_count % 1000000 == 0:
                logger.info(f"Processed {line_count:,} lines, found {total_domains:,} unique domains")
            
            line = line.strip()
            if not line or line.startswith(";"):
                continue
            
            parts = line.split()
            if len(parts) < 4:
                continue
            
            owner = parts[0].lower()
            record_type = parts[3].lower()
            rdata = parts[4] if len(parts) > 4 else ""
            
            if owner == f"{self.tld.lower()}." or owner == self.tld.lower():
                continue
            
            if owner.endswith(tld_suffix_lower):
                domain = owner[:-len(tld_suffix_lower)]
                
                if not domain or "." in domain:
                    continue
                
                is_new = domain not in domains
                if is_new:
                    domains[domain] = DomainRecord(domain=domain)
                    total_domains += 1
                
                record = domains[domain]
                
                if record_type == "ns" and rdata and rdata not in record.ns:
                    record.ns.append(rdata.rstrip("."))
                elif record_type == "a" and rdata and rdata not in record.a:
                    record.a.append(rdata)
                elif record_type == "aaaa" and rdata and rdata not in record.aaaa:
                    record.aaaa.append(rdata)
                elif record_type == "ds" and rdata:
                    ds_data = " ".join(parts[4:])
                    if ds_data not in record.ds:
                        record.ds.append(ds_data)
                
                if len(domains) >= self.chunk_size:
                    logger.info(f"Yielding chunk of {len(domains):,} domains")
                    yield domains, False
                    domains = {}
        
        if domains:
            yield domains, True
        
        logger.info(
            f"Parsed {self.file_path.name}: {line_count:,} lines, "
            f"{total_domains:,} unique domains"
        )
    
    def parse_domains(self) -> Dict[str, DomainRecord]:
        """
        Parse all domains at once (legacy method for small files).
        WARNING: May cause OOM on large files.
        """
        all_domains = {}
        for chunk, is_last in self.parse_domains_chunked():
            all_domains.update(chunk)
        return all_domains


def parse_zone_file(file_path: Path) -> Tuple[str, Dict[str, DomainRecord]]:
    """
    Convenience function to parse a zone file (all at once).
    Returns tuple of (tld, domains_dict).
    WARNING: Use parse_zone_file_chunked for large files.
    """
    parser = ZoneParser(file_path)
    domains = parser.parse_domains()
    return parser.tld, domains


def parse_zone_file_chunked(
    file_path: Path, 
    chunk_size: int = CHUNK_SIZE
) -> Generator[Tuple[str, Dict[str, DomainRecord], bool], None, None]:
    """
    Parse zone file in chunks (memory efficient).
    
    Yields:
        Tuple of (tld, domains_dict, is_last_chunk)
    """
    parser = ZoneParser(file_path, chunk_size)
    for chunk, is_last in parser.parse_domains_chunked():
        yield parser.tld, chunk, is_last
