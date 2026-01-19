"""
Database module for Cornerstone Care Payroll Auditor.
Uses SQLite for storing staff, clients, and assignments.
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

# Database file location
DB_PATH = os.path.join(os.path.dirname(__file__), "payroll_data.db")


def get_connection():
    """Get a database connection with row factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """Initialize the database with all required tables."""
    conn = get_connection()
    cursor = conn.cursor()

    # Staff table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staff (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Family groups table (for linking siblings/family members)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS family_groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Clients table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            pos_hours REAL NOT NULL DEFAULT 0,
            family_group_id INTEGER,
            is_private INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (family_group_id) REFERENCES family_groups(id)
        )
    """)

    # Assignments table (who works for whom, and their assigned hours)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_id INTEGER NOT NULL,
            client_id INTEGER NOT NULL,
            assigned_hours REAL NOT NULL,
            is_permanent INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (staff_id) REFERENCES staff(id),
            FOREIGN KEY (client_id) REFERENCES clients(id),
            UNIQUE(staff_id, client_id)
        )
    """)

    # Timesheet entries table (for private client hours from photos)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS timesheet_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            staff_id INTEGER NOT NULL,
            client_id INTEGER NOT NULL,
            service_date TEXT NOT NULL,
            shift_in TEXT,
            shift_out TEXT,
            hours REAL NOT NULL,
            week_number INTEGER,
            pay_period_start TEXT,
            source TEXT DEFAULT 'MANUAL',
            ai_confidence REAL,
            reviewed_by TEXT,
            is_approved INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (staff_id) REFERENCES staff(id),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        )
    """)

    conn.commit()
    conn.close()


# =============================================================================
# STAFF CRUD OPERATIONS
# =============================================================================

def get_all_staff(active_only: bool = True) -> List[Dict]:
    """Get all staff members."""
    conn = get_connection()
    cursor = conn.cursor()

    if active_only:
        cursor.execute("SELECT * FROM staff WHERE is_active = 1 ORDER BY name")
    else:
        cursor.execute("SELECT * FROM staff ORDER BY name")

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_staff_by_id(staff_id: int) -> Optional[Dict]:
    """Get a single staff member by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM staff WHERE id = ?", (staff_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_staff_by_name(name: str) -> Optional[Dict]:
    """Get a single staff member by name (case-insensitive)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM staff WHERE LOWER(name) = LOWER(?)", (name,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def add_staff(name: str) -> int:
    """Add a new staff member. Returns the new ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO staff (name) VALUES (?)", (name.strip(),))
    conn.commit()
    new_id = cursor.lastrowid
    conn.close()
    return new_id


def update_staff(staff_id: int, name: str = None, is_active: bool = None):
    """Update a staff member."""
    conn = get_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    if name is not None:
        updates.append("name = ?")
        params.append(name.strip())
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(1 if is_active else 0)

    if updates:
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(staff_id)
        cursor.execute(f"UPDATE staff SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()

    conn.close()


def delete_staff(staff_id: int):
    """Soft delete a staff member (set inactive)."""
    update_staff(staff_id, is_active=False)


# =============================================================================
# FAMILY GROUPS CRUD OPERATIONS
# =============================================================================

def get_all_family_groups() -> List[Dict]:
    """Get all family groups."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM family_groups ORDER BY name")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_family_group(name: str, notes: str = None) -> int:
    """Add a new family group. Returns the new ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO family_groups (name, notes) VALUES (?, ?)",
        (name.strip(), notes)
    )
    conn.commit()
    new_id = cursor.lastrowid
    conn.close()
    return new_id


def delete_family_group(group_id: int):
    """Delete a family group (will unlink any clients)."""
    conn = get_connection()
    cursor = conn.cursor()
    # First unlink any clients
    cursor.execute("UPDATE clients SET family_group_id = NULL WHERE family_group_id = ?", (group_id,))
    # Then delete the group
    cursor.execute("DELETE FROM family_groups WHERE id = ?", (group_id,))
    conn.commit()
    conn.close()


# =============================================================================
# CLIENTS CRUD OPERATIONS
# =============================================================================

def get_all_clients(active_only: bool = True) -> List[Dict]:
    """Get all clients with their family group info."""
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT c.*, fg.name as family_group_name
        FROM clients c
        LEFT JOIN family_groups fg ON c.family_group_id = fg.id
    """

    if active_only:
        query += " WHERE c.is_active = 1"

    query += " ORDER BY c.name"

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_client_by_id(client_id: int) -> Optional[Dict]:
    """Get a single client by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.*, fg.name as family_group_name
        FROM clients c
        LEFT JOIN family_groups fg ON c.family_group_id = fg.id
        WHERE c.id = ?
    """, (client_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_client_by_name(name: str) -> Optional[Dict]:
    """Get a single client by name (case-insensitive)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.*, fg.name as family_group_name
        FROM clients c
        LEFT JOIN family_groups fg ON c.family_group_id = fg.id
        WHERE LOWER(c.name) = LOWER(?)
    """, (name,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def add_client(name: str, pos_hours: float, is_private: bool = False,
               family_group_id: int = None, notes: str = None) -> int:
    """Add a new client. Returns the new ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO clients (name, pos_hours, is_private, family_group_id, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (name.strip(), pos_hours, 1 if is_private else 0, family_group_id, notes))
    conn.commit()
    new_id = cursor.lastrowid
    conn.close()
    return new_id


def update_client(client_id: int, name: str = None, pos_hours: float = None,
                  is_private: bool = None, family_group_id: int = None,
                  is_active: bool = None, notes: str = None):
    """Update a client."""
    conn = get_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    if name is not None:
        updates.append("name = ?")
        params.append(name.strip())
    if pos_hours is not None:
        updates.append("pos_hours = ?")
        params.append(pos_hours)
    if is_private is not None:
        updates.append("is_private = ?")
        params.append(1 if is_private else 0)
    if family_group_id is not None:
        updates.append("family_group_id = ?")
        params.append(family_group_id if family_group_id > 0 else None)
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(1 if is_active else 0)
    if notes is not None:
        updates.append("notes = ?")
        params.append(notes)

    if updates:
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(client_id)
        cursor.execute(f"UPDATE clients SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()

    conn.close()


def delete_client(client_id: int):
    """Soft delete a client (set inactive)."""
    update_client(client_id, is_active=False)


def get_clients_by_family_group(family_group_id: int) -> List[Dict]:
    """Get all clients in a family group."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM clients
        WHERE family_group_id = ? AND is_active = 1
        ORDER BY name
    """, (family_group_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


# =============================================================================
# ASSIGNMENTS CRUD OPERATIONS
# =============================================================================

def get_all_assignments() -> List[Dict]:
    """Get all assignments with staff and client names."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.*, s.name as staff_name, c.name as client_name, c.pos_hours
        FROM assignments a
        JOIN staff s ON a.staff_id = s.id
        JOIN clients c ON a.client_id = c.id
        WHERE s.is_active = 1 AND c.is_active = 1
        ORDER BY c.name, s.name
    """)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_assignments_by_client(client_id: int) -> List[Dict]:
    """Get all assignments for a specific client."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.*, s.name as staff_name
        FROM assignments a
        JOIN staff s ON a.staff_id = s.id
        WHERE a.client_id = ? AND s.is_active = 1
        ORDER BY s.name
    """, (client_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_assignments_by_staff(staff_id: int) -> List[Dict]:
    """Get all assignments for a specific staff member."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.*, c.name as client_name, c.pos_hours
        FROM assignments a
        JOIN clients c ON a.client_id = c.id
        WHERE a.staff_id = ? AND c.is_active = 1
        ORDER BY c.name
    """, (staff_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_assignment(staff_id: int, client_id: int) -> Optional[Dict]:
    """Get a specific assignment."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.*, s.name as staff_name, c.name as client_name, c.pos_hours
        FROM assignments a
        JOIN staff s ON a.staff_id = s.id
        JOIN clients c ON a.client_id = c.id
        WHERE a.staff_id = ? AND a.client_id = ?
    """, (staff_id, client_id))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def add_assignment(staff_id: int, client_id: int, assigned_hours: float,
                   is_permanent: bool = True, notes: str = None) -> int:
    """Add a new assignment. Returns the new ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO assignments (staff_id, client_id, assigned_hours, is_permanent, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (staff_id, client_id, assigned_hours, 1 if is_permanent else 0, notes))
    conn.commit()
    new_id = cursor.lastrowid
    conn.close()
    return new_id


def update_assignment(assignment_id: int, assigned_hours: float = None,
                      is_permanent: bool = None, notes: str = None):
    """Update an assignment."""
    conn = get_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    if assigned_hours is not None:
        updates.append("assigned_hours = ?")
        params.append(assigned_hours)
    if is_permanent is not None:
        updates.append("is_permanent = ?")
        params.append(1 if is_permanent else 0)
    if notes is not None:
        updates.append("notes = ?")
        params.append(notes)

    if updates:
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(assignment_id)
        cursor.execute(f"UPDATE assignments SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()

    conn.close()


def upsert_assignment(staff_id: int, client_id: int, assigned_hours: float,
                      is_permanent: bool = True, notes: str = None) -> int:
    """Add or update an assignment. Returns the ID."""
    existing = get_assignment(staff_id, client_id)
    if existing:
        update_assignment(existing['id'], assigned_hours, is_permanent, notes)
        return existing['id']
    else:
        return add_assignment(staff_id, client_id, assigned_hours, is_permanent, notes)


def delete_assignment(assignment_id: int):
    """Delete an assignment."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM assignments WHERE id = ?", (assignment_id,))
    conn.commit()
    conn.close()


# =============================================================================
# IMPORT HELPERS
# =============================================================================

def import_staff_from_names(names: List[str]) -> Dict[str, int]:
    """
    Import staff from a list of names.
    Returns a dict mapping names to IDs (existing or newly created).
    """
    result = {}
    for name in names:
        name = name.strip()
        if not name:
            continue

        existing = get_staff_by_name(name)
        if existing:
            result[name] = existing['id']
        else:
            result[name] = add_staff(name)

    return result


def import_clients_from_names(names: List[str], default_pos: float = 0) -> Dict[str, int]:
    """
    Import clients from a list of names.
    Returns a dict mapping names to IDs (existing or newly created).
    """
    result = {}
    for name in names:
        name = name.strip()
        if not name:
            continue

        existing = get_client_by_name(name)
        if existing:
            result[name] = existing['id']
        else:
            result[name] = add_client(name, default_pos)

    return result


# =============================================================================
# LOOKUP HELPERS (for payroll calculation)
# =============================================================================

def get_client_pos_map() -> Dict[str, float]:
    """Get a mapping of client names to their POS hours."""
    clients = get_all_clients()
    return {c['name']: c['pos_hours'] for c in clients}


def get_assignment_map() -> Dict[tuple, float]:
    """
    Get a mapping of (staff_name, client_name) to assigned hours.
    Used for payroll calculation.
    """
    assignments = get_all_assignments()
    return {(a['staff_name'], a['client_name']): a['assigned_hours'] for a in assignments}


def get_staff_name_map() -> Dict[str, int]:
    """Get a mapping of staff names (lowercase) to IDs."""
    staff = get_all_staff()
    return {s['name'].lower(): s['id'] for s in staff}


def get_client_name_map() -> Dict[str, int]:
    """Get a mapping of client names (lowercase) to IDs."""
    clients = get_all_clients()
    return {c['name'].lower(): c['id'] for c in clients}


# Initialize database when module is imported
init_database()
