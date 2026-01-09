#!/usr/bin/env python3
"""
Build offline address database from OpenStreetMap data.

This script downloads OSM data for a region (US state, country, or city),
extracts searchable places (addresses, POIs, cities, etc.), and creates
a SQLite database with FTS5 for fast text search and R*Tree for spatial queries.

Requirements:
    pip install osmium requests

Usage:
    # US States
    python build_state_db.py virginia
    python build_state_db.py --all-us

    # Worldwide regions (from Geofabrik)
    python build_state_db.py --region europe/germany
    python build_state_db.py --region asia/japan
    python build_state_db.py --region africa/egypt

    # Cities (via Overpass API - smaller areas only)
    python build_state_db.py --city "London, UK" --bbox -0.5,51.3,0.3,51.7
    python build_state_db.py --city "Tokyo, Japan" --bbox 139.5,35.5,140.0,36.0

    # Custom PBF file
    python build_state_db.py --file my-region.osm.pbf --name "My Region"
"""

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import time
import urllib.request
import urllib.parse
from pathlib import Path

# Check for osmium
try:
    import osmium
except ImportError:
    print("ERROR: osmium not installed. Install with: pip install osmium")
    print("You may also need: apt install libosmium2-dev (Linux) or brew install libosmium (macOS)")
    sys.exit(1)

# ============================================================================
# US States
# ============================================================================

US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district-of-columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new-hampshire": "NH", "new-jersey": "NJ", "new-mexico": "NM",
    "new-york": "NY", "north-carolina": "NC", "north-dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode-island": "RI",
    "south-carolina": "SC", "south-dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west-virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}

# ============================================================================
# Geofabrik regions (partial list - most common)
# Full list at: https://download.geofabrik.de/
# ============================================================================

GEOFABRIK_REGIONS = {
    # North America
    "north-america/us": "https://download.geofabrik.de/north-america/us",
    "north-america/canada": "https://download.geofabrik.de/north-america/canada",
    "north-america/mexico": "https://download.geofabrik.de/north-america/mexico",
    
    # Europe
    "europe/germany": "https://download.geofabrik.de/europe/germany",
    "europe/france": "https://download.geofabrik.de/europe/france",
    "europe/great-britain": "https://download.geofabrik.de/europe/great-britain",
    "europe/italy": "https://download.geofabrik.de/europe/italy",
    "europe/spain": "https://download.geofabrik.de/europe/spain",
    "europe/poland": "https://download.geofabrik.de/europe/poland",
    "europe/netherlands": "https://download.geofabrik.de/europe/netherlands",
    "europe/belgium": "https://download.geofabrik.de/europe/belgium",
    "europe/switzerland": "https://download.geofabrik.de/europe/switzerland",
    "europe/austria": "https://download.geofabrik.de/europe/austria",
    "europe/sweden": "https://download.geofabrik.de/europe/sweden",
    "europe/norway": "https://download.geofabrik.de/europe/norway",
    "europe/finland": "https://download.geofabrik.de/europe/finland",
    "europe/denmark": "https://download.geofabrik.de/europe/denmark",
    "europe/portugal": "https://download.geofabrik.de/europe/portugal",
    "europe/greece": "https://download.geofabrik.de/europe/greece",
    "europe/ireland-and-northern-ireland": "https://download.geofabrik.de/europe/ireland-and-northern-ireland",
    "europe/ukraine": "https://download.geofabrik.de/europe/ukraine",
    "europe/romania": "https://download.geofabrik.de/europe/romania",
    "europe/czech-republic": "https://download.geofabrik.de/europe/czech-republic",
    
    # Asia
    "asia/japan": "https://download.geofabrik.de/asia/japan",
    "asia/south-korea": "https://download.geofabrik.de/asia/south-korea",
    "asia/taiwan": "https://download.geofabrik.de/asia/taiwan",
    "asia/philippines": "https://download.geofabrik.de/asia/philippines",
    "asia/thailand": "https://download.geofabrik.de/asia/thailand",
    "asia/vietnam": "https://download.geofabrik.de/asia/vietnam",
    "asia/malaysia-singapore-brunei": "https://download.geofabrik.de/asia/malaysia-singapore-brunei",
    "asia/indonesia": "https://download.geofabrik.de/asia/indonesia",
    "asia/india": "https://download.geofabrik.de/asia/india",
    "asia/pakistan": "https://download.geofabrik.de/asia/pakistan",
    "asia/israel-and-palestine": "https://download.geofabrik.de/asia/israel-and-palestine",
    "asia/iraq": "https://download.geofabrik.de/asia/iraq",
    "asia/iran": "https://download.geofabrik.de/asia/iran",
    "asia/afghanistan": "https://download.geofabrik.de/asia/afghanistan",
    
    # South America
    "south-america/brazil": "https://download.geofabrik.de/south-america/brazil",
    "south-america/argentina": "https://download.geofabrik.de/south-america/argentina",
    "south-america/chile": "https://download.geofabrik.de/south-america/chile",
    "south-america/colombia": "https://download.geofabrik.de/south-america/colombia",
    "south-america/peru": "https://download.geofabrik.de/south-america/peru",
    
    # Africa
    "africa/egypt": "https://download.geofabrik.de/africa/egypt",
    "africa/south-africa": "https://download.geofabrik.de/africa/south-africa",
    "africa/morocco": "https://download.geofabrik.de/africa/morocco",
    "africa/kenya": "https://download.geofabrik.de/africa/kenya",
    "africa/nigeria": "https://download.geofabrik.de/africa/nigeria",
    
    # Oceania
    "australia-oceania/australia": "https://download.geofabrik.de/australia-oceania/australia",
    "australia-oceania/new-zealand": "https://download.geofabrik.de/australia-oceania/new-zealand",
    
    # Russia
    "russia": "https://download.geofabrik.de/russia",
}

# ============================================================================
# POI Categories (matching Android app's PointOfInterestType enum)
# ============================================================================

POI_CATEGORIES = {
    # Medical/Emergency
    "HOSPITAL": ("amenity", "hospital"),
    "PHARMACY": ("amenity", "pharmacy"),
    "DENTIST": ("amenity", "dentist"),
    "DOCTOR": ("amenity", "doctors"),
    "CLINIC": ("amenity", "clinic"),
    "VETERINARIAN": ("amenity", "veterinary"),
    
    # Emergency Services
    "FIRE_STATION": ("amenity", "fire_station"),
    "POLICE_STATION": ("amenity", "police"),
    
    # Transportation
    "AIRPORT": ("aeroway", "aerodrome"),
    "HELIPORT": ("aeroway", "helipad"),
    "RAILWAY_STATION": ("railway", "station"),
    "FERRY_TERMINAL": ("amenity", "ferry_terminal"),
    "PARKING": ("amenity", "parking"),
    
    # Fuel/Services
    "GAS_STATION": ("amenity", "fuel"),
    "CAR_WASH": ("amenity", "car_wash"),
    
    # Finance
    "BANK": ("amenity", "bank"),
    "ATM": ("amenity", "atm"),
    
    # Education
    "SCHOOL": ("amenity", "school"),
    
    # Food/Lodging
    "RESTAURANT": ("amenity", "restaurant"),
    "HOTEL": ("tourism", "hotel"),
    "CAFE": ("amenity", "cafe"),
    "FAST_FOOD": ("amenity", "fast_food"),
    "BAR": ("amenity", "bar"),
    "PUB": ("amenity", "pub"),
    
    # Shopping/Groceries
    "SUPERMARKET": ("shop", "supermarket"),
    "CONVENIENCE_STORE": ("shop", "convenience"),
    "SHOPPING_MALL": ("shop", "mall"),
    "HARDWARE_STORE": ("shop", "hardware"),
    
    # Services
    "LAUNDRY": ("shop", "laundry"),
    "HAIR_SALON": ("shop", "hairdresser"),
    
    # Entertainment/Recreation
    "CINEMA": ("amenity", "cinema"),
    "GYM": ("leisure", "fitness_centre"),
    
    # Cemetery/Memorial
    "CEMETERY": ("landuse", "cemetery"),
    "GRAVE_YARD": ("amenity", "grave_yard"),
    
    # Surveillance/Security
    "SURVEILLANCE": ("man_made", "surveillance"),
    
    # Government/Diplomatic
    "EMBASSY": ("amenity", "embassy"),
    "GOVERNMENT": ("office", "government"),
    "PRISON": ("amenity", "prison"),
    
    # Communications/Infrastructure
    "COMM_TOWER": ("man_made", "tower"),
    "CELL_TOWER": ("man_made", "mast"),
    "POWER_STATION": ("power", "plant"),
    "WATER_TOWER": ("man_made", "water_tower"),
    
    # Utilities
    "POST_OFFICE": ("amenity", "post_office"),
    
    # Community
    "PLACE_OF_WORSHIP": ("amenity", "place_of_worship"),
    "LIBRARY": ("amenity", "library"),
}

POI_TAG_TO_CATEGORY = {v: k for k, v in POI_CATEGORIES.items()}


# ============================================================================
# OSM Data Handler
# ============================================================================

class PlaceHandler(osmium.SimpleHandler):
    """Extract searchable places and POIs from OSM data."""
    
    def __init__(self, region_name=""):
        super().__init__()
        self.places = []
        self.pois = []
        self.region_name = region_name
        self.place_count = 0
        self.poi_count = 0
        
    def _extract_place(self, obj, lat, lon):
        """Extract place info from an OSM object."""
        tags = dict(obj.tags)
        
        name = tags.get('name')
        addr_street = tags.get('addr:street')
        addr_housenumber = tags.get('addr:housenumber')
        
        if not name and not addr_street:
            return None
            
        place_type = self._get_place_type(tags)
        if not place_type:
            return None
            
        display_name = self._build_display_name(tags, name)
        short_name = name or f"{addr_housenumber} {addr_street}".strip()
        
        place = {
            'osm_id': obj.id,
            'osm_type': 'node' if hasattr(obj, 'location') else 'way',
            'lat': lat,
            'lon': lon,
            'name': short_name,
            'display_name': display_name,
            'type': place_type,
            'street': addr_street or '',
            'housenumber': addr_housenumber or '',
            'city': tags.get('addr:city', ''),
            'postcode': tags.get('addr:postcode', ''),
            'state': tags.get('addr:state', self.region_name),
            'country': tags.get('addr:country', ''),
        }
        
        return place
    
    def _extract_poi(self, obj, lat, lon):
        """Extract POI with category information."""
        tags = dict(obj.tags)
        
        poi_category = None
        for (osm_key, osm_value), category in POI_TAG_TO_CATEGORY.items():
            if tags.get(osm_key) == osm_value:
                poi_category = category
                break
        
        if not poi_category:
            return None
        
        name = tags.get('name', '')
        
        addr_parts = []
        if tags.get('addr:housenumber'):
            addr_parts.append(tags['addr:housenumber'])
        if tags.get('addr:street'):
            addr_parts.append(tags['addr:street'])
        if tags.get('addr:city'):
            addr_parts.append(tags['addr:city'])
        if tags.get('addr:postcode'):
            addr_parts.append(tags['addr:postcode'])
        address = ', '.join(addr_parts) if addr_parts else ''
        
        poi = {
            'osm_id': obj.id,
            'osm_type': 'node' if hasattr(obj, 'location') else 'way',
            'lat': lat,
            'lon': lon,
            'name': name,
            'category': poi_category,
            'address': address,
            'phone': tags.get('phone', tags.get('contact:phone', '')),
            'website': tags.get('website', tags.get('contact:website', '')),
            'opening_hours': tags.get('opening_hours', ''),
        }
        
        return poi
    
    def _get_place_type(self, tags):
        """Determine the type of place from OSM tags."""
        place = tags.get('place')
        if place in ('city', 'town', 'village', 'hamlet', 'suburb', 'neighbourhood', 'locality'):
            return place
            
        amenity = tags.get('amenity')
        if amenity:
            return amenity
            
        shop = tags.get('shop')
        if shop:
            return f"shop_{shop}"
            
        tourism = tags.get('tourism')
        if tourism:
            return tourism
            
        if tags.get('addr:street') and tags.get('addr:housenumber'):
            building = tags.get('building')
            if building:
                return f"building_{building}" if building != 'yes' else 'building'
            return 'address'
            
        leisure = tags.get('leisure')
        if leisure:
            return leisure
            
        office = tags.get('office')
        if office:
            return f"office_{office}"
        
        aeroway = tags.get('aeroway')
        if aeroway:
            return aeroway
        
        railway = tags.get('railway')
        if railway:
            return railway
            
        if tags.get('name'):
            landuse = tags.get('landuse')
            if landuse:
                return landuse
                
        return None
    
    def _build_display_name(self, tags, name):
        """Build a full display name from tags."""
        parts = []
        
        if name:
            parts.append(name)
            
        street = tags.get('addr:street')
        housenumber = tags.get('addr:housenumber')
        if street:
            if housenumber:
                parts.append(f"{housenumber} {street}")
            elif not name:
                parts.append(street)
                
        city = tags.get('addr:city')
        if city:
            parts.append(city)
            
        state = tags.get('addr:state')
        if state:
            parts.append(state)
            
        country = tags.get('addr:country')
        if country:
            parts.append(country)
            
        postcode = tags.get('addr:postcode')
        if postcode:
            parts.append(postcode)
            
        return ', '.join(parts) if parts else self.region_name
    
    def node(self, n):
        if not n.location.valid():
            return
        
        lat = n.location.lat
        lon = n.location.lon
        
        place = self._extract_place(n, lat, lon)
        if place:
            self.places.append(place)
            self.place_count += 1
            if self.place_count % 50000 == 0:
                print(f"  Processed {self.place_count} places...")
        
        poi = self._extract_poi(n, lat, lon)
        if poi:
            self.pois.append(poi)
            self.poi_count += 1
    
    def way(self, w):
        try:
            lats = []
            lons = []
            for node in w.nodes:
                if node.location.valid():
                    lats.append(node.location.lat)
                    lons.append(node.location.lon)
            
            if not lats:
                return
                
            lat = sum(lats) / len(lats)
            lon = sum(lons) / len(lons)
            
            place = self._extract_place(w, lat, lon)
            if place:
                place['osm_type'] = 'way'
                self.places.append(place)
                self.place_count += 1
                if self.place_count % 50000 == 0:
                    print(f"  Processed {self.place_count} places...")
            
            poi = self._extract_poi(w, lat, lon)
            if poi:
                poi['osm_type'] = 'way'
                self.pois.append(poi)
                self.poi_count += 1
                
        except Exception:
            pass


# ============================================================================
# Download Functions
# ============================================================================

def download_with_progress(url, output_path):
    """Download a file with progress indicator."""
    print(f"  Downloading: {url}")
    print(f"  This may take a while for large regions...")
    
    def reporthook(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            percent = min(100, downloaded * 100 / total_size)
            mb_down = downloaded / (1024 * 1024)
            mb_total = total_size / (1024 * 1024)
            print(f"\r  Downloaded: {mb_down:.1f} MB / {mb_total:.1f} MB ({percent:.1f}%)", end='', flush=True)
    
    urllib.request.urlretrieve(url, output_path, reporthook)
    print()


def download_us_state(state_key, cache_dir):
    """Download OSM data for a US state."""
    url = f"https://download.geofabrik.de/north-america/us/{state_key}-latest.osm.pbf"
    output_path = cache_dir / f"us-{state_key}.osm.pbf"
    
    if output_path.exists():
        print(f"  Using cached: {output_path}")
        return output_path
    
    download_with_progress(url, output_path)
    return output_path


def download_geofabrik_region(region_key, cache_dir):
    """Download OSM data for a Geofabrik region."""
    # Handle direct URL or key lookup
    if region_key.startswith("http"):
        base_url = region_key
        region_name = region_key.split('/')[-1]
    elif region_key in GEOFABRIK_REGIONS:
        base_url = GEOFABRIK_REGIONS[region_key]
        region_name = region_key.replace('/', '-')
    else:
        # Try to construct URL
        base_url = f"https://download.geofabrik.de/{region_key}"
        region_name = region_key.replace('/', '-')
    
    url = f"{base_url}-latest.osm.pbf"
    output_path = cache_dir / f"{region_name}.osm.pbf"
    
    if output_path.exists():
        print(f"  Using cached: {output_path}")
        return output_path
    
    download_with_progress(url, output_path)
    return output_path


def download_city_bbox(city_name, bbox, cache_dir):
    """Download OSM data for a city using Overpass API."""
    west, south, east, north = bbox
    
    # Validate bbox size (Overpass API has limits)
    area = (east - west) * (north - south)
    if area > 1.0:  # ~1 degree squared, roughly 100x100 km at equator
        print(f"  WARNING: Bounding box is large ({area:.2f} sq deg). Consider using Geofabrik region instead.")
    
    safe_name = city_name.lower().replace(' ', '-').replace(',', '').replace('/', '-')
    output_path = cache_dir / f"city-{safe_name}.osm.pbf"
    
    if output_path.exists():
        print(f"  Using cached: {output_path}")
        return output_path
    
    # First download as XML, then note that osmium can handle XML directly
    xml_path = cache_dir / f"city-{safe_name}.osm"
    
    print(f"  Downloading via Overpass API: {city_name}")
    print(f"  Bounding box: {west},{south},{east},{north}")
    print(f"  This may take several minutes...")
    
    # Overpass query for all data in bbox
    query = f"""
    [out:xml][timeout:600][bbox:{south},{west},{north},{east}];
    (
      node;
      way;
      relation;
    );
    out body;
    >;
    out skel qt;
    """
    
    url = "https://overpass-api.de/api/interpreter"
    data = urllib.parse.urlencode({'data': query}).encode('utf-8')
    
    try:
        req = urllib.request.Request(url, data=data)
        req.add_header('User-Agent', 'ATAK-Address-Plugin-DB-Builder/1.0')
        
        with urllib.request.urlopen(req, timeout=660) as response:
            with open(xml_path, 'wb') as f:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
        
        print(f"  Downloaded: {xml_path}")
        return xml_path  # osmium can read XML files directly
        
    except Exception as e:
        print(f"  ERROR: Overpass API request failed: {e}")
        print(f"  Try using a Geofabrik region instead, or reduce the bounding box size.")
        return None


# ============================================================================
# Database Creation
# ============================================================================

def create_database(places, pois, output_path, region_name):
    """Create SQLite database with FTS5 and R*Tree."""
    print(f"  Creating database with {len(places)} places and {len(pois)} POIs...")
    
    if output_path.exists():
        output_path.unlink()
    
    conn = sqlite3.connect(str(output_path))
    cursor = conn.cursor()
    
    # Places table
    cursor.execute('''
        CREATE TABLE places (
            id INTEGER PRIMARY KEY,
            osm_id INTEGER,
            osm_type TEXT,
            lat REAL,
            lon REAL,
            name TEXT,
            display_name TEXT,
            type TEXT,
            street TEXT,
            housenumber TEXT,
            city TEXT,
            postcode TEXT,
            state TEXT,
            country TEXT
        )
    ''')
    
    # FTS5 for text search
    cursor.execute('''
        CREATE VIRTUAL TABLE places_fts USING fts5(
            name,
            display_name,
            street,
            city,
            postcode,
            content='places',
            content_rowid='id'
        )
    ''')
    
    # Insert places
    cursor.executemany('''
        INSERT INTO places (osm_id, osm_type, lat, lon, name, display_name, type,
                           street, housenumber, city, postcode, state, country)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
        (p['osm_id'], p['osm_type'], p['lat'], p['lon'], p['name'], p['display_name'],
         p['type'], p['street'], p['housenumber'], p['city'], p['postcode'], 
         p['state'], p.get('country', ''))
        for p in places
    ])
    
    # Populate FTS
    cursor.execute('''
        INSERT INTO places_fts (rowid, name, display_name, street, city, postcode)
        SELECT id, name, display_name, street, city, postcode FROM places
    ''')
    
    cursor.execute('CREATE INDEX idx_places_type ON places(type)')
    cursor.execute('CREATE INDEX idx_places_city ON places(city)')
    
    # POI table
    cursor.execute('''
        CREATE TABLE pois (
            id INTEGER PRIMARY KEY,
            osm_id INTEGER,
            osm_type TEXT,
            lat REAL,
            lon REAL,
            name TEXT,
            category TEXT,
            address TEXT,
            phone TEXT,
            website TEXT,
            opening_hours TEXT
        )
    ''')
    
    # R*Tree for spatial queries
    cursor.execute('''
        CREATE VIRTUAL TABLE pois_rtree USING rtree(
            id,
            min_lat, max_lat,
            min_lon, max_lon
        )
    ''')
    
    # Insert POIs
    cursor.executemany('''
        INSERT INTO pois (osm_id, osm_type, lat, lon, name, category, address, phone, website, opening_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
        (p['osm_id'], p['osm_type'], p['lat'], p['lon'], p['name'], p['category'],
         p['address'], p['phone'], p['website'], p['opening_hours'])
        for p in pois
    ])
    
    cursor.execute('''
        INSERT INTO pois_rtree (id, min_lat, max_lat, min_lon, max_lon)
        SELECT id, lat, lat, lon, lon FROM pois
    ''')
    
    cursor.execute('CREATE INDEX idx_pois_category ON pois(category)')
    
    # Metadata
    cursor.execute('''
        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    import datetime
    cursor.execute('INSERT INTO metadata VALUES (?, ?)', 
                   ('created', datetime.datetime.now().isoformat()))
    cursor.execute('INSERT INTO metadata VALUES (?, ?)',
                   ('place_count', str(len(places))))
    cursor.execute('INSERT INTO metadata VALUES (?, ?)',
                   ('poi_count', str(len(pois))))
    cursor.execute('INSERT INTO metadata VALUES (?, ?)',
                   ('region', region_name))
    cursor.execute('INSERT INTO metadata VALUES (?, ?)',
                   ('schema_version', '2'))
    
    conn.commit()
    cursor.execute('VACUUM')
    cursor.execute('ANALYZE')
    conn.close()
    
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Database created: {output_path} ({size_mb:.1f} MB)")
    
    return output_path


# ============================================================================
# Build Functions
# ============================================================================

def build_database(pbf_path, output_path, region_name):
    """Build database from a PBF/OSM file."""
    print(f"\n[2/3] Extracting places and POIs...")
    print(f"  This may take several minutes...")
    
    handler = PlaceHandler(region_name)
    handler.apply_file(str(pbf_path), locations=True)
    
    print(f"  Found {len(handler.places)} searchable places")
    print(f"  Found {len(handler.pois)} POIs")
    
    print(f"\n[3/3] Creating SQLite database...")
    create_database(handler.places, handler.pois, output_path, region_name)
    
    # Statistics
    type_counts = {}
    for p in handler.places:
        t = p['type']
        type_counts[t] = type_counts.get(t, 0) + 1
    
    print("\n  Top place types:")
    for t, count in sorted(type_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"    {t}: {count:,}")
    
    poi_counts = {}
    for p in handler.pois:
        c = p['category']
        poi_counts[c] = poi_counts.get(c, 0) + 1
    
    if poi_counts:
        print("\n  POI categories:")
        for c, count in sorted(poi_counts.items(), key=lambda x: -x[1]):
            print(f"    {c}: {count:,}")
    
    return output_path


def build_manifest(output_dir):
    """Build manifest.json listing all databases."""
    databases = list(output_dir.glob("*.db"))
    
    manifest = {
        "version": "2.0",
        "schema_version": 2,
        "poi_categories": list(POI_CATEGORIES.keys()),
        "states": []
    }
    
    for db_path in sorted(databases):
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        place_count = 0
        poi_count = 0
        region = db_path.stem
        
        try:
            cursor.execute("SELECT value FROM metadata WHERE key='place_count'")
            row = cursor.fetchone()
            if row:
                place_count = int(row[0])
            
            cursor.execute("SELECT value FROM metadata WHERE key='poi_count'")
            row = cursor.fetchone()
            if row:
                poi_count = int(row[0])
            
            cursor.execute("SELECT value FROM metadata WHERE key='region'")
            row = cursor.fetchone()
            if row:
                region = row[0]
        except:
            pass
        
        conn.close()
        
        manifest["states"].append({
            "id": db_path.stem,
            "name": region,
            "size": db_path.stat().st_size,
            "place_count": place_count,
            "poi_count": poi_count,
            "filename": db_path.name,
        })
    
    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\nManifest created: {manifest_path}")
    return manifest_path


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Build offline address/POI database from OSM data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # US States
  python build_state_db.py virginia
  python build_state_db.py new-york
  python build_state_db.py --all-us

  # Worldwide regions (from Geofabrik)
  python build_state_db.py --region europe/germany
  python build_state_db.py --region asia/japan
  python build_state_db.py --region africa/egypt

  # Cities (via Overpass API)
  python build_state_db.py --city "London" --bbox -0.5,51.3,0.3,51.7
  python build_state_db.py --city "Paris" --bbox 2.2,48.8,2.5,48.95

  # Custom PBF file
  python build_state_db.py --file my-region.osm.pbf --name "My Region"

Available Geofabrik regions:
  """ + '\n  '.join(sorted(GEOFABRIK_REGIONS.keys()))
    )
    
    parser.add_argument('state', nargs='?', help='US state name (e.g., "virginia")')
    parser.add_argument('--all-us', action='store_true', help='Build all US states')
    parser.add_argument('--region', help='Geofabrik region (e.g., "europe/germany")')
    parser.add_argument('--city', help='City name for Overpass download')
    parser.add_argument('--bbox', help='Bounding box: west,south,east,north')
    parser.add_argument('--file', type=Path, help='Custom PBF/OSM file')
    parser.add_argument('--name', help='Region name for custom file')
    parser.add_argument('--output-dir', type=Path, default=Path('output'))
    parser.add_argument('--cache-dir', type=Path, default=Path('cache'))
    parser.add_argument('--list-regions', action='store_true', help='List available regions')
    
    args = parser.parse_args()
    
    if args.list_regions:
        print("Available Geofabrik regions:")
        for r in sorted(GEOFABRIK_REGIONS.keys()):
            print(f"  {r}")
        print("\nUS States:")
        for s in sorted(US_STATES.keys()):
            print(f"  {s}")
        return
    
    args.output_dir.mkdir(exist_ok=True)
    args.cache_dir.mkdir(exist_ok=True)
    
    # US State
    if args.state:
        state_key = args.state.lower().replace(' ', '-')
        if state_key not in US_STATES:
            print(f"Unknown state: {args.state}")
            print(f"Try: {', '.join(sorted(US_STATES.keys())[:5])}...")
            return
        
        print(f"\n{'='*60}")
        print(f"Building: {args.state.title()} ({US_STATES[state_key]})")
        print(f"{'='*60}")
        
        print("\n[1/3] Downloading OSM data...")
        pbf_path = download_us_state(state_key, args.cache_dir)
        db_path = args.output_dir / f"{state_key}.db"
        build_database(pbf_path, db_path, US_STATES[state_key])
        build_manifest(args.output_dir)
    
    # All US
    elif args.all_us:
        for state_key in US_STATES:
            try:
                print(f"\n{'='*60}")
                print(f"Building: {state_key.title()}")
                print(f"{'='*60}")
                print("\n[1/3] Downloading OSM data...")
                pbf_path = download_us_state(state_key, args.cache_dir)
                db_path = args.output_dir / f"{state_key}.db"
                build_database(pbf_path, db_path, US_STATES[state_key])
            except Exception as e:
                print(f"ERROR: {e}")
        build_manifest(args.output_dir)
    
    # Geofabrik region
    elif args.region:
        region_name = args.region.split('/')[-1].replace('-', ' ').title()
        safe_name = args.region.replace('/', '-')
        
        print(f"\n{'='*60}")
        print(f"Building: {region_name}")
        print(f"{'='*60}")
        
        print("\n[1/3] Downloading OSM data...")
        pbf_path = download_geofabrik_region(args.region, args.cache_dir)
        if pbf_path:
            db_path = args.output_dir / f"{safe_name}.db"
            build_database(pbf_path, db_path, region_name)
            build_manifest(args.output_dir)
    
    # City via Overpass
    elif args.city and args.bbox:
        try:
            bbox = [float(x) for x in args.bbox.split(',')]
            if len(bbox) != 4:
                raise ValueError("Need 4 values")
        except:
            print("ERROR: --bbox must be: west,south,east,north")
            print("Example: --bbox -0.5,51.3,0.3,51.7")
            return
        
        safe_name = args.city.lower().replace(' ', '-').replace(',', '')
        
        print(f"\n{'='*60}")
        print(f"Building: {args.city}")
        print(f"{'='*60}")
        
        print("\n[1/3] Downloading OSM data via Overpass API...")
        osm_path = download_city_bbox(args.city, bbox, args.cache_dir)
        if osm_path:
            db_path = args.output_dir / f"city-{safe_name}.db"
            build_database(osm_path, db_path, args.city)
            build_manifest(args.output_dir)
    
    # Custom file
    elif args.file:
        if not args.file.exists():
            print(f"ERROR: File not found: {args.file}")
            return
        
        name = args.name or args.file.stem
        safe_name = name.lower().replace(' ', '-')
        
        print(f"\n{'='*60}")
        print(f"Building: {name}")
        print(f"{'='*60}")
        
        print("\n[1/3] Using provided file...")
        db_path = args.output_dir / f"{safe_name}.db"
        build_database(args.file, db_path, name)
        build_manifest(args.output_dir)
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
