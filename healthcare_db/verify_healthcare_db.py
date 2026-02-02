import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_step(msg):
    print(f"{Colors.BLUE}{msg}{Colors.END}")


def print_success(msg):
    print(f"{Colors.GREEN}✓ {msg}{Colors.END}")


def print_error(msg):
    print(f"{Colors.RED}✗ {msg}{Colors.END}")


def get_connection():
    config = {
        'user': os.getenv('HEALTHCARE_USER'),
        'password': os.getenv('HEALTHCARE_PASSWORD'),
        'host': os.getenv('HEALTHCARE_HOST'),
        'port': os.getenv('HEALTHCARE_PORT'),
        'database': os.getenv('HEALTHCARE_DATABASE'),
    }
    return psycopg2.connect(**config)


def test_connection():
    print_step("Testing database connection...")
    try:
        conn = get_connection()
        conn.close()
        print_success("Connection successful")
        return True
    except Exception as e:
        print_error(f"Connection failed: {e}")
        return False


def test_tables():
    print_step("Checking tables...")
    
    expected_tables = [
        'insurance_providers', 'patients', 'departments', 'doctors',
        'medical_records', 'appointments', 'lab_results', 'prescriptions'
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        
        existing_tables = [row[0] for row in cur.fetchall()]
        
        all_exist = True
        for table in expected_tables:
            if table in existing_tables:
                print_success(f"Table '{table}' exists")
            else:
                print_error(f"Table '{table}' missing")
                all_exist = False
        
        cur.close()
        conn.close()
        return all_exist
        
    except Exception as e:
        print_error(f"Error checking tables: {e}")
        return False


def test_foreign_keys():
    print_step("Checking foreign key constraints...")
    
    expected_fks = [
        ('patients', 'patients_insurance_provider_id_fkey'),
        ('doctors', 'doctors_department_id_fkey'),
        ('medical_records', 'medical_records_patient_id_fkey'),
        ('medical_records', 'medical_records_doctor_id_fkey'),
        ('appointments', 'appointments_patient_id_fkey'),
        ('appointments', 'appointments_doctor_id_fkey'),
        ('lab_results', 'lab_results_patient_id_fkey'),
        ('lab_results', 'lab_results_doctor_id_fkey'),
        ('prescriptions', 'prescriptions_patient_id_fkey'),
        ('prescriptions', 'prescriptions_doctor_id_fkey'),
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        all_exist = True
        for table, constraint in expected_fks:
            cur.execute("""
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = %s AND constraint_name = %s
            """, (table, constraint))
            
            if cur.fetchone():
                print_success(f"FK '{constraint}' exists")
            else:
                print_error(f"FK '{constraint}' missing")
                all_exist = False
        
        cur.close()
        conn.close()
        return all_exist
        
    except Exception as e:
        print_error(f"Error checking foreign keys: {e}")
        return False


def test_data_population():
    print_step("Checking data population...")
    
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        tables = ['insurance_providers', 'patients', 'doctors', 'appointments', 
                  'medical_records', 'prescriptions']
        all_populated = True
        
        for table in tables:
            cur.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cur.fetchone()['count']
            
            if count > 0:
                print_success(f"Table '{table}' has {count} rows")
            else:
                print_error(f"Table '{table}' is empty")
                all_populated = False
        
        cur.close()
        conn.close()
        return all_populated
        
    except Exception as e:
        print_error(f"Error checking data: {e}")
        return False


def test_sample_queries():
    print_step("Testing sample queries...")
    
    queries = [
        ("Simple SELECT", "SELECT COUNT(*) as count FROM patients"),
        
        ("JOIN query", """
            SELECT COUNT(*) as count 
            FROM appointments a 
            JOIN patients p ON a.patient_id = p.patient_id
        """),
        
        ("Department aggregation", """
            SELECT d.department_name, COUNT(doc.doctor_id) as doctor_count
            FROM departments d
            LEFT JOIN doctors doc ON d.department_id = doc.department_id
            GROUP BY d.department_name
            LIMIT 5
        """),
        
        ("Complex join with prescriptions", """
            SELECT p.first_name, p.last_name, COUNT(pr.prescription_id) as prescription_count
            FROM patients p
            LEFT JOIN prescriptions pr ON p.patient_id = pr.patient_id
            GROUP BY p.patient_id
            LIMIT 5
        """),
        
        ("Appointments by status", """
            SELECT status, COUNT(*) as count
            FROM appointments
            GROUP BY status
        """),
        
        ("Patients with insurance", """
            SELECT COUNT(*) as count
            FROM patients
            WHERE insurance_provider_id IS NOT NULL
        """),
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        all_passed = True
        for name, query in queries:
            try:
                cur.execute(query)
                cur.fetchall()
                print_success(f"Query '{name}' executed successfully")
            except Exception as e:
                print_error(f"Query '{name}' failed: {e}")
                all_passed = False
        
        cur.close()
        conn.close()
        return all_passed
        
    except Exception as e:
        print_error(f"Error running queries: {e}")
        return False


def test_indexes():
    print_step("Checking indexes...")
    
    expected_indexes = [
        'idx_patients_last_name',
        'idx_patients_insurance',
        'idx_doctors_department',
        'idx_appointments_patient',
        'idx_appointments_doctor',
        'idx_appointments_date',
    ]
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        all_exist = True
        for index in expected_indexes:
            cur.execute("""
                SELECT 1 FROM pg_indexes
                WHERE indexname = %s
            """, (index,))
            
            if cur.fetchone():
                print_success(f"Index '{index}' exists")
            else:
                print_error(f"Index '{index}' missing")
                all_exist = False
        
        cur.close()
        conn.close()
        return all_exist
        
    except Exception as e:
        print_error(f"Error checking indexes: {e}")
        return False


def test_data_integrity():
    print_step("Checking data integrity...")
    
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Test 1: All patients have valid insurance provider if insurance_provider_id is set
        cur.execute("""
            SELECT COUNT(*) as count
            FROM patients p
            WHERE p.insurance_provider_id IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM insurance_providers ip 
                WHERE ip.provider_id = p.insurance_provider_id
            )
        """)
        orphan_insurance = cur.fetchone()['count']
        
        if orphan_insurance == 0:
            print_success("No orphaned insurance references")
        else:
            print_error(f"Found {orphan_insurance} orphaned insurance references")
        
        # Test 2: All appointments reference valid patients and doctors
        cur.execute("""
            SELECT COUNT(*) as count
            FROM appointments a
            WHERE NOT EXISTS (SELECT 1 FROM patients p WHERE p.patient_id = a.patient_id)
            OR NOT EXISTS (SELECT 1 FROM doctors d WHERE d.doctor_id = a.doctor_id)
        """)
        orphan_appointments = cur.fetchone()['count']
        
        if orphan_appointments == 0:
            print_success("All appointments have valid patient and doctor references")
        else:
            print_error(f"Found {orphan_appointments} appointments with invalid references")
        
        # Test 3: Check for reasonable date ranges
        cur.execute("""
            SELECT COUNT(*) as count
            FROM patients
            WHERE date_of_birth > CURRENT_DATE
            OR date_of_birth < '1900-01-01'
        """)
        invalid_dobs = cur.fetchone()['count']
        
        if invalid_dobs == 0:
            print_success("All patient birth dates are valid")
        else:
            print_error(f"Found {invalid_dobs} patients with invalid birth dates")
        
        # Test 4: Check appointment times are reasonable
        cur.execute("""
            SELECT COUNT(*) as count
            FROM appointments
            WHERE appointment_time < '06:00:00'
            OR appointment_time > '22:00:00'
        """)
        odd_times = cur.fetchone()['count']
        
        if odd_times == 0:
            print_success("All appointment times are within reasonable hours")
        else:
            print_error(f"Found {odd_times} appointments outside normal hours")
        
        cur.close()
        conn.close()
        
        return (orphan_insurance == 0 and orphan_appointments == 0 and 
                invalid_dobs == 0 and odd_times == 0)
        
    except Exception as e:
        print_error(f"Error checking data integrity: {e}")
        return False


def main():
    print(f"\n{Colors.BOLD}Healthcare Database Verification{Colors.END}\n")
    
    tests = []
    
    # Run all tests
    tests.append(("Connection", test_connection()))
    
    if not tests[-1][1]:
        print(f"\n{Colors.RED}Cannot continue without database connection{Colors.END}")
        sys.exit(1)
    
    tests.append(("Tables exist", test_tables()))
    tests.append(("Foreign keys", test_foreign_keys()))
    tests.append(("Data populated", test_data_population()))
    tests.append(("Sample queries", test_sample_queries()))
    tests.append(("Indexes", test_indexes()))
    tests.append(("Data integrity", test_data_integrity()))
    
    # Summary
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}Test Summary{Colors.END}")
    print(f"{Colors.BOLD}{'=' * 60}{Colors.END}\n")
    
    passed = sum(1 for _, result in tests if result)
    total = len(tests)
    
    for name, result in tests:
        status = f"{Colors.GREEN}✓ PASS" if result else f"{Colors.RED}✗ FAIL"
        print(f"{status}{Colors.END} - {name}")
    
    print(f"\n{Colors.BOLD}Results: {passed}/{total} tests passed{Colors.END}\n")
    
    if passed == total:
        print(f"{Colors.GREEN}{Colors.BOLD}✓ All tests passed! Database is ready.{Colors.END}")
        print(f"\n{Colors.BOLD}Next steps:{Colors.END}")
        print("  1. Build knowledge graph for healthcare_db")
        print("  2. Test natural language queries about patients, appointments, and doctors\n")
        sys.exit(0)
    else:
        print(f"{Colors.RED}{Colors.BOLD}✗ Some tests failed.{Colors.END}\n")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Colors.BOLD}Verification cancelled{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)