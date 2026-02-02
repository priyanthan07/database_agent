import os
import sys
from datetime import datetime, timedelta, time
import random
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()


class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_step(message: str):
    print(f"{Colors.BLUE}{Colors.BOLD}► {message}{Colors.END}")


def print_success(message: str):
    print(f"{Colors.GREEN}✓ {message}{Colors.END}")


def print_error(message: str):
    print(f"{Colors.RED}✗ {message}{Colors.END}")


def get_config():
    """Get database configuration from environment."""
    return {
        'user': os.getenv('HEALTHCARE_USER'),
        'password': os.getenv('HEALTHCARE_PASSWORD'),
        'host': os.getenv('HEALTHCARE_HOST'),
        'port': os.getenv('HEALTHCARE_PORT'),
        'database': os.getenv('HEALTHCARE_DATABASE'),
    }


def create_database(config):
    """Create the healthcare_db database if it doesn't exist."""
    print_step(f"Creating database '{config['database']}'...")
    
    try:
        conn = psycopg2.connect(
            user=config['user'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (config['database'],)
        )
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(f"CREATE DATABASE {config['database']}")
            print_success(f"Database '{config['database']}' created")
        else:
            print_success(f"Database '{config['database']}' already exists")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print_error(f"Error creating database: {e}")
        return False


def create_schema(config):
    """Create all tables for the healthcare database."""
    print_step("Creating database schema...")
    
    conn = psycopg2.connect(
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['database']
    )
    cur = conn.cursor()
    
    # Drop existing tables (in reverse order of dependencies)
    print_step("Dropping existing tables if any...")
    cur.execute("""
        DROP TABLE IF EXISTS prescriptions CASCADE;
        DROP TABLE IF EXISTS lab_results CASCADE;
        DROP TABLE IF EXISTS appointments CASCADE;
        DROP TABLE IF EXISTS medical_records CASCADE;
        DROP TABLE IF EXISTS doctors CASCADE;
        DROP TABLE IF EXISTS departments CASCADE;
        DROP TABLE IF EXISTS patients CASCADE;
        DROP TABLE IF EXISTS insurance_providers CASCADE;
    """)
    
    # Create insurance_providers table
    cur.execute("""
        CREATE TABLE insurance_providers (
            provider_id SERIAL PRIMARY KEY,
            provider_name VARCHAR(200) NOT NULL,
            contact_phone VARCHAR(20),
            contact_email VARCHAR(100),
            coverage_type VARCHAR(50),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created insurance_providers table")
    
    # Create patients table
    cur.execute("""
        CREATE TABLE patients (
            patient_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            date_of_birth DATE NOT NULL,
            gender VARCHAR(20) CHECK (gender IN ('Male', 'Female', 'Other', 'Prefer not to say')),
            blood_type VARCHAR(5),
            email VARCHAR(150),
            phone VARCHAR(20) NOT NULL,
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(50),
            zip_code VARCHAR(10),
            emergency_contact_name VARCHAR(200),
            emergency_contact_phone VARCHAR(20),
            insurance_provider_id INTEGER REFERENCES insurance_providers(provider_id) ON DELETE SET NULL,
            insurance_policy_number VARCHAR(50),
            is_active BOOLEAN DEFAULT TRUE,
            registration_date DATE DEFAULT CURRENT_DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created patients table")
    
    # Create departments table
    cur.execute("""
        CREATE TABLE departments (
            department_id SERIAL PRIMARY KEY,
            department_name VARCHAR(150) NOT NULL,
            floor_number INTEGER,
            building VARCHAR(50),
            head_doctor_name VARCHAR(200),
            phone_extension VARCHAR(10),
            description TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created departments table")
    
    # Create doctors table
    cur.execute("""
        CREATE TABLE doctors (
            doctor_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            specialization VARCHAR(150) NOT NULL,
            department_id INTEGER REFERENCES departments(department_id) ON DELETE SET NULL,
            license_number VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(150),
            phone VARCHAR(20),
            years_of_experience INTEGER,
            consultation_fee DECIMAL(8, 2),
            is_available BOOLEAN DEFAULT TRUE,
            hire_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created doctors table")
    
    # Create medical_records table
    cur.execute("""
        CREATE TABLE medical_records (
            record_id SERIAL PRIMARY KEY,
            patient_id INTEGER NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
            doctor_id INTEGER REFERENCES doctors(doctor_id) ON DELETE SET NULL,
            visit_date DATE NOT NULL,
            diagnosis TEXT NOT NULL,
            symptoms TEXT,
            treatment_plan TEXT,
            notes TEXT,
            follow_up_required BOOLEAN DEFAULT FALSE,
            follow_up_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created medical_records table")
    
    # Create appointments table
    cur.execute("""
        CREATE TABLE appointments (
            appointment_id SERIAL PRIMARY KEY,
            patient_id INTEGER NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
            doctor_id INTEGER NOT NULL REFERENCES doctors(doctor_id) ON DELETE CASCADE,
            appointment_date DATE NOT NULL,
            appointment_time TIME NOT NULL,
            duration_minutes INTEGER DEFAULT 30,
            appointment_type VARCHAR(50) CHECK (appointment_type IN ('Consultation', 'Follow-up', 'Emergency', 'Routine Checkup', 'Surgery')),
            status VARCHAR(30) DEFAULT 'Scheduled' CHECK (status IN ('Scheduled', 'Confirmed', 'In Progress', 'Completed', 'Cancelled', 'No Show')),
            reason_for_visit TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created appointments table")
    
    # Create lab_results table
    cur.execute("""
        CREATE TABLE lab_results (
            result_id SERIAL PRIMARY KEY,
            patient_id INTEGER NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
            doctor_id INTEGER REFERENCES doctors(doctor_id) ON DELETE SET NULL,
            test_name VARCHAR(200) NOT NULL,
            test_date DATE NOT NULL,
            result_value VARCHAR(100),
            unit_of_measure VARCHAR(50),
            reference_range VARCHAR(100),
            status VARCHAR(30) DEFAULT 'Pending' CHECK (status IN ('Pending', 'Completed', 'Abnormal', 'Critical')),
            lab_technician_name VARCHAR(200),
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created lab_results table")
    
    # Create prescriptions table
    cur.execute("""
        CREATE TABLE prescriptions (
            prescription_id SERIAL PRIMARY KEY,
            patient_id INTEGER NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
            doctor_id INTEGER NOT NULL REFERENCES doctors(doctor_id) ON DELETE CASCADE,
            medication_name VARCHAR(200) NOT NULL,
            dosage VARCHAR(100) NOT NULL,
            frequency VARCHAR(100) NOT NULL,
            duration_days INTEGER,
            quantity INTEGER,
            refills_allowed INTEGER DEFAULT 0,
            prescription_date DATE NOT NULL,
            start_date DATE,
            end_date DATE,
            instructions TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print_success("Created prescriptions table")
    
    # Create indexes for better query performance
    print_step("Creating indexes...")
    cur.execute("CREATE INDEX idx_patients_last_name ON patients(last_name)")
    cur.execute("CREATE INDEX idx_patients_insurance ON patients(insurance_provider_id)")
    cur.execute("CREATE INDEX idx_doctors_department ON doctors(department_id)")
    cur.execute("CREATE INDEX idx_doctors_specialization ON doctors(specialization)")
    cur.execute("CREATE INDEX idx_appointments_patient ON appointments(patient_id)")
    cur.execute("CREATE INDEX idx_appointments_doctor ON appointments(doctor_id)")
    cur.execute("CREATE INDEX idx_appointments_date ON appointments(appointment_date)")
    cur.execute("CREATE INDEX idx_appointments_status ON appointments(status)")
    cur.execute("CREATE INDEX idx_medical_records_patient ON medical_records(patient_id)")
    cur.execute("CREATE INDEX idx_medical_records_doctor ON medical_records(doctor_id)")
    cur.execute("CREATE INDEX idx_lab_results_patient ON lab_results(patient_id)")
    cur.execute("CREATE INDEX idx_prescriptions_patient ON prescriptions(patient_id)")
    cur.execute("CREATE INDEX idx_prescriptions_doctor ON prescriptions(doctor_id)")
    print_success("Created indexes")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print_success("Schema created successfully")
    return True


def populate_sample_data(config):
    """Populate tables with realistic healthcare sample data."""
    print_step("Populating sample data...")
    
    conn = psycopg2.connect(
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['database']
    )
    cur = conn.cursor()
    
    # Sample data
    first_names = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
                   'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
                   'Thomas', 'Sarah', 'Charles', 'Karen', 'Daniel', 'Nancy', 'Matthew', 'Lisa']
    
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
                  'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White']
    
    # Insert insurance providers
    print_step("Inserting insurance providers...")
    insurance_data = [
        ('BlueCross BlueShield', '1-800-555-0101', 'bcbs@insurance.com', 'PPO'),
        ('Aetna Health', '1-800-555-0102', 'aetna@insurance.com', 'HMO'),
        ('UnitedHealthcare', '1-800-555-0103', 'united@insurance.com', 'PPO'),
        ('Cigna', '1-800-555-0104', 'cigna@insurance.com', 'HMO'),
        ('Humana', '1-800-555-0105', 'humana@insurance.com', 'Medicare'),
        ('Kaiser Permanente', '1-800-555-0106', 'kaiser@insurance.com', 'HMO'),
        ('Anthem', '1-800-555-0107', 'anthem@insurance.com', 'PPO'),
        ('Medicare', '1-800-555-0108', 'medicare@gov.com', 'Government'),
    ]
    
    provider_ids = []
    for name, phone, email, coverage in insurance_data:
        cur.execute("""
            INSERT INTO insurance_providers (provider_name, contact_phone, contact_email, coverage_type)
            VALUES (%s, %s, %s, %s)
            RETURNING provider_id
        """, (name, phone, email, coverage))
        provider_ids.append(cur.fetchone()[0])
    
    print_success(f"Inserted {len(insurance_data)} insurance providers")
    
    # Insert patients
    print_step("Inserting patients...")
    blood_types = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
    genders = ['Male', 'Female', 'Other', 'Prefer not to say']
    cities = ['Boston', 'New York', 'Philadelphia', 'Chicago', 'Los Angeles', 'Houston']
    states = ['MA', 'NY', 'PA', 'IL', 'CA', 'TX']
    
    patient_ids = []
    for i in range(100):
        first = random.choice(first_names)
        last = random.choice(last_names)
        
        # Age between 1 and 90 years
        age_days = random.randint(365, 365 * 90)
        dob = datetime.now().date() - timedelta(days=age_days)
        
        gender = random.choice(genders)
        blood_type = random.choice(blood_types)
        email = f"{first.lower()}.{last.lower()}{i}@email.com"
        phone = f"+1-555-{random.randint(1000, 9999)}"
        
        city_idx = random.randint(0, len(cities) - 1)
        address = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Maple', 'Pine'])} St"
        
        emergency_name = f"{random.choice(first_names)} {random.choice(last_names)}"
        emergency_phone = f"+1-555-{random.randint(1000, 9999)}"
        
        # 80% have insurance
        insurance_id = random.choice(provider_ids) if random.random() < 0.8 else None
        policy_number = f"POL-{random.randint(100000, 999999)}" if insurance_id else None
        
        reg_days_ago = random.randint(1, 1825)  # Up to 5 years ago
        reg_date = datetime.now().date() - timedelta(days=reg_days_ago)
        
        cur.execute("""
            INSERT INTO patients (
                first_name, last_name, date_of_birth, gender, blood_type,
                email, phone, address, city, state, zip_code,
                emergency_contact_name, emergency_contact_phone,
                insurance_provider_id, insurance_policy_number, registration_date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING patient_id
        """, (
            first, last, dob, gender, blood_type, email, phone, address,
            cities[city_idx], states[city_idx], f"{random.randint(10000, 99999)}",
            emergency_name, emergency_phone, insurance_id, policy_number, reg_date
        ))
        
        patient_ids.append(cur.fetchone()[0])
    
    print_success(f"Inserted {len(patient_ids)} patients")
    
    # Insert departments
    print_step("Inserting departments...")
    departments_data = [
        ('Cardiology', 3, 'Main Building', 'Dr. Robert Chen'),
        ('Neurology', 4, 'Main Building', 'Dr. Sarah Miller'),
        ('Orthopedics', 2, 'West Wing', 'Dr. James Wilson'),
        ('Pediatrics', 1, 'East Wing', 'Dr. Linda Garcia'),
        ('Emergency Medicine', 1, 'Main Building', 'Dr. Michael Brown'),
        ('Internal Medicine', 2, 'Main Building', 'Dr. Patricia Davis'),
        ('Oncology', 5, 'Research Building', 'Dr. David Martinez'),
        ('Dermatology', 3, 'West Wing', 'Dr. Jennifer Taylor'),
        ('Psychiatry', 4, 'East Wing', 'Dr. William Anderson'),
        ('Radiology', 1, 'Diagnostic Center', 'Dr. Elizabeth Thomas'),
    ]
    
    department_ids = []
    for name, floor, building, head in departments_data:
        cur.execute("""
            INSERT INTO departments (department_name, floor_number, building, head_doctor_name, phone_extension)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING department_id
        """, (name, floor, building, head, f"x{random.randint(1000, 9999)}"))
        department_ids.append(cur.fetchone()[0])
    
    print_success(f"Inserted {len(departments_data)} departments")
    
    # Insert doctors
    print_step("Inserting doctors...")
    specializations = [
        'Cardiologist', 'Neurologist', 'Orthopedic Surgeon', 'Pediatrician',
        'Emergency Physician', 'Internist', 'Oncologist', 'Dermatologist',
        'Psychiatrist', 'Radiologist', 'General Practitioner', 'Surgeon'
    ]
    
    doctor_ids = []
    for i in range(40):
        first = random.choice(first_names)
        last = random.choice(last_names)
        spec = random.choice(specializations)
        dept_id = random.choice(department_ids)
        license_num = f"MD-{random.randint(100000, 999999)}"
        email = f"dr.{last.lower()}{i}@hospital.com"
        phone = f"+1-555-{random.randint(1000, 9999)}"
        years_exp = random.randint(3, 35)
        fee = Decimal(str(random.randint(100, 500)))
        
        hire_days_ago = random.randint(365, 365 * 20)
        hire_date = datetime.now().date() - timedelta(days=hire_days_ago)
        
        cur.execute("""
            INSERT INTO doctors (
                first_name, last_name, specialization, department_id, license_number,
                email, phone, years_of_experience, consultation_fee, hire_date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING doctor_id
        """, (first, last, spec, dept_id, license_num, email, phone, years_exp, fee, hire_date))
        
        doctor_ids.append(cur.fetchone()[0])
    
    print_success(f"Inserted {len(doctor_ids)} doctors")
    
    # Insert appointments
    print_step("Inserting appointments...")
    appointment_types = ['Consultation', 'Follow-up', 'Emergency', 'Routine Checkup', 'Surgery']
    statuses = ['Scheduled', 'Confirmed', 'In Progress', 'Completed', 'Cancelled', 'No Show']
    
    total_appointments = 0
    for patient_id in patient_ids:
        # Each patient has 1-5 appointments
        num_appts = random.randint(1, 5)
        
        for _ in range(num_appts):
            doctor_id = random.choice(doctor_ids)
            
            # Appointments within last 180 days or next 60 days
            days_offset = random.randint(-180, 60)
            appt_date = datetime.now().date() + timedelta(days=days_offset)
            
            # Appointment time during business hours
            hour = random.randint(8, 17)
            minute = random.choice([0, 15, 30, 45])
            appt_time = time(hour, minute)
            
            duration = random.choice([15, 30, 45, 60])
            appt_type = random.choice(appointment_types)
            
            # Past appointments more likely completed
            if days_offset < 0:
                status = random.choice(['Completed', 'Completed', 'Completed', 'Cancelled', 'No Show'])
            else:
                status = random.choice(['Scheduled', 'Confirmed'])
            
            reason = random.choice([
                'Annual checkup', 'Follow-up visit', 'Chest pain', 'Back pain',
                'Skin rash', 'Headaches', 'Fever', 'Routine examination'
            ])
            
            cur.execute("""
                INSERT INTO appointments (
                    patient_id, doctor_id, appointment_date, appointment_time,
                    duration_minutes, appointment_type, status, reason_for_visit
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (patient_id, doctor_id, appt_date, appt_time, duration, appt_type, status, reason))
            
            total_appointments += 1
    
    print_success(f"Inserted {total_appointments} appointments")
    
    # Insert medical records
    print_step("Inserting medical records...")
    diagnoses = [
        'Hypertension', 'Type 2 Diabetes', 'Common Cold', 'Influenza', 'Migraine',
        'Lower Back Pain', 'Anxiety Disorder', 'Depression', 'Asthma', 'Allergic Rhinitis',
        'Gastroesophageal Reflux Disease', 'Osteoarthritis', 'Coronary Artery Disease'
    ]
    
    total_records = 0
    for patient_id in patient_ids:
        # Each patient has 1-4 medical records
        num_records = random.randint(1, 4)
        
        for _ in range(num_records):
            doctor_id = random.choice(doctor_ids)
            days_ago = random.randint(1, 730)
            visit_date = datetime.now().date() - timedelta(days=days_ago)
            
            diagnosis = random.choice(diagnoses)
            symptoms = random.choice([
                'Persistent cough, fever', 'Chest discomfort, shortness of breath',
                'Severe headache, nausea', 'Joint pain, stiffness', 'Fatigue, dizziness'
            ])
            
            treatment = random.choice([
                'Prescribed medication, rest', 'Physical therapy recommended',
                'Lifestyle modifications advised', 'Further testing required',
                'Surgical consultation scheduled'
            ])
            
            follow_up = random.choice([True, False])
            follow_up_date = visit_date + timedelta(days=random.randint(7, 30)) if follow_up else None
            
            cur.execute("""
                INSERT INTO medical_records (
                    patient_id, doctor_id, visit_date, diagnosis, symptoms,
                    treatment_plan, follow_up_required, follow_up_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (patient_id, doctor_id, visit_date, diagnosis, symptoms, treatment, follow_up, follow_up_date))
            
            total_records += 1
    
    print_success(f"Inserted {total_records} medical records")
    
    # Insert lab results
    print_step("Inserting lab results...")
    lab_tests = [
        ('Complete Blood Count', 'WBC', '7.5', 'K/uL', '4.5-11.0'),
        ('Blood Glucose', 'Glucose', '95', 'mg/dL', '70-100'),
        ('Cholesterol Panel', 'Total Cholesterol', '185', 'mg/dL', '<200'),
        ('Liver Function Test', 'ALT', '25', 'U/L', '7-56'),
        ('Thyroid Panel', 'TSH', '2.5', 'mIU/L', '0.4-4.0'),
        ('Hemoglobin A1C', 'HbA1c', '5.6', '%', '<5.7'),
        ('Creatinine', 'Creatinine', '0.9', 'mg/dL', '0.7-1.3'),
    ]
    
    lab_statuses = ['Pending', 'Completed', 'Completed', 'Completed', 'Abnormal']
    
    total_labs = 0
    for patient_id in random.sample(patient_ids, k=60):  # 60% of patients have lab results
        num_labs = random.randint(1, 3)
        
        for _ in range(num_labs):
            doctor_id = random.choice(doctor_ids)
            days_ago = random.randint(1, 365)
            test_date = datetime.now().date() - timedelta(days=days_ago)
            
            test_name, value_name, value, unit, ref_range = random.choice(lab_tests)
            status = random.choice(lab_statuses)
            tech_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            
            cur.execute("""
                INSERT INTO lab_results (
                    patient_id, doctor_id, test_name, test_date, result_value,
                    unit_of_measure, reference_range, status, lab_technician_name
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (patient_id, doctor_id, test_name, test_date, value, unit, ref_range, status, tech_name))
            
            total_labs += 1
    
    print_success(f"Inserted {total_labs} lab results")
    
    # Insert prescriptions
    print_step("Inserting prescriptions...")
    medications = [
        ('Lisinopril', '10 mg', 'Once daily', 90),
        ('Metformin', '500 mg', 'Twice daily', 90),
        ('Atorvastatin', '20 mg', 'Once daily at bedtime', 90),
        ('Levothyroxine', '50 mcg', 'Once daily in the morning', 90),
        ('Omeprazole', '20 mg', 'Once daily before breakfast', 30),
        ('Ibuprofen', '400 mg', 'Every 6 hours as needed', 14),
        ('Amoxicillin', '500 mg', 'Three times daily', 10),
        ('Prednisone', '10 mg', 'Once daily with food', 7),
    ]
    
    total_prescriptions = 0
    for patient_id in patient_ids:
        # Each patient has 0-3 prescriptions
        num_rx = random.randint(0, 3)
        
        for _ in range(num_rx):
            doctor_id = random.choice(doctor_ids)
            days_ago = random.randint(1, 365)
            rx_date = datetime.now().date() - timedelta(days=days_ago)
            
            med_name, dosage, frequency, duration = random.choice(medications)
            quantity = random.randint(30, 90)
            refills = random.randint(0, 3)
            
            start_date = rx_date
            end_date = start_date + timedelta(days=duration) if duration else None
            
            instructions = random.choice([
                'Take with food', 'Take on empty stomach', 'Do not crush or chew',
                'May cause drowsiness', 'Complete full course'
            ])
            
            # Active if end date is in future or no end date
            is_active = end_date is None or end_date >= datetime.now().date()
            
            cur.execute("""
                INSERT INTO prescriptions (
                    patient_id, doctor_id, medication_name, dosage, frequency,
                    duration_days, quantity, refills_allowed, prescription_date,
                    start_date, end_date, instructions, is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (patient_id, doctor_id, med_name, dosage, frequency, duration, quantity,
                  refills, rx_date, start_date, end_date, instructions, is_active))
            
            total_prescriptions += 1
    
    print_success(f"Inserted {total_prescriptions} prescriptions")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print_success("Sample data populated successfully")
    return True


def print_summary(config):
    """Print database summary."""
    conn = psycopg2.connect(
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['database']
    )
    cur = conn.cursor()
    
    # Get row counts
    tables = ['insurance_providers', 'patients', 'departments', 'doctors', 
              'medical_records', 'appointments', 'lab_results', 'prescriptions']
    counts = {}
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.GREEN}✓ Healthcare Database Ready!{Colors.END}")
    print(f"{Colors.BOLD}{'=' * 60}{Colors.END}\n")
    
    print(f"{Colors.BOLD}Database: {config['database']}{Colors.END}")
    print(f"Host: {config['host']}:{config['port']}\n")
    
    print(f"{Colors.BOLD}Table Summary:{Colors.END}")
    for table in tables:
        print(f"  {table:25s} {counts[table]:6d} rows")
    
    print(f"\n{Colors.BOLD}Sample Queries to Try:{Colors.END}")
    print("  1. 'Show me all appointments scheduled for next week'")
    print("  2. 'Which patients have appointments with Dr. Smith?'")
    print("  3. 'List all patients with diabetes diagnosis'")
    print("  4. 'What are the most common diagnoses?'")
    print("  5. 'Show me all pending lab results'")
    
    print(f"\n{Colors.BOLD}Connection String:{Colors.END}")
    print(f"  postgresql://{config['user']}:****@{config['host']}:{config['port']}/{config['database']}\n")


def main():
    """Main setup function."""
    print(f"\n{Colors.BOLD}Healthcare Database Setup{Colors.END}\n")
    
    config = get_config()
    
    # Step 1: Create database
    if not create_database(config):
        sys.exit(1)
    
    # Step 2: Create schema
    if not create_schema(config):
        sys.exit(1)
    
    # Step 3: Populate data
    if not populate_sample_data(config):
        sys.exit(1)
    
    # Step 4: Print summary
    print_summary(config)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Setup cancelled{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)