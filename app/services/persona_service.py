import email
from typing import Sequence
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..models.persona import Persona
from ..views.persona import PersonaCreate, PersonaUpdate
from .errors import PersonaNotFoundError, EmailAlreadyExistsError

##Se importa libreria faker y se importa random
from faker import Faker
import random

## Se importa la libreria sqlalchemy text
from sqlalchemy import text

##se importa la libreria sqllchemy or, funcion Persona 
from sqlalchemy import or_
from ..models.persona import Persona

##se importan librerias list, dict y se importa persona 
from typing import List, Dict
from ..models.persona import Persona    



def create_persona(db: Session, payload: PersonaCreate) -> Persona:
    """Create a Persona ensuring unique email."""
    # Optimistic check; DB unique constraint is the final guard
    if db.query(Persona).filter(Persona.email == payload.email).first():
        raise EmailAlreadyExistsError()
    obj = Persona(
        first_name=payload.first_name,
        last_name=payload.last_name,
        email=payload.email,
        phone=payload.phone,
        birth_date=payload.birth_date,
        is_active=payload.is_active,
        notes=payload.notes,
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        # Catch race conditions on unique email
        raise EmailAlreadyExistsError() from e
    db.refresh(obj)
    return obj


def list_personas(db: Session, skip: int = 0, limit: int = 100) -> Sequence[Persona]:
    """Return paginated list of Personas."""
    return db.query(Persona).offset(skip).limit(limit).all()


def get_persona(db: Session, persona_id: int) -> Persona:
    """Return Persona by ID or raise if not found."""
    obj = db.query(Persona).filter(Persona.id == persona_id).first()
    if not obj:
        raise PersonaNotFoundError()
    return obj


def update_persona(db: Session, persona_id: int, payload: PersonaUpdate) -> Persona:
    """Update Persona partially, enforcing unique email."""
    obj = db.query(Persona).filter(Persona.id == persona_id).first()
    if not obj:
        raise PersonaNotFoundError()

    data = payload.model_dump(exclude_unset=True)
    if "email" in data and data["email"] != obj.email:
        if db.query(Persona).filter(Persona.email == data["email"], Persona.id != persona_id).first():
            raise EmailAlreadyExistsError()

    for field, value in data.items():
        setattr(obj, field, value)

    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise EmailAlreadyExistsError() from e
    db.refresh(obj)
    return obj


def delete_persona(db: Session, persona_id: int) -> None:
    """Delete Persona by ID or raise if not found."""
    obj = db.query(Persona).filter(Persona.id == persona_id).first()
    if not obj:
        raise PersonaNotFoundError()
    db.delete(obj)
    db.commit()

## Funcion para poblar la base de datos con datos falsos
faker=Faker("es_Co")

def populate_personas(db, cantidad: int):
    dominios = ["gmail.com", "hotmail.com", "outlook.com"]

    for _ in range(cantidad):
        first_name = faker.first_name() or faker.first_name_male()
        last_name = faker.last_name() or faker.last_name_male()

        persona = Persona(
            first_name=first_name,
            last_name=last_name,
            email=f"{first_name.lower()}.{last_name.lower()}.{faker.uuid4()}@{random.choice(dominios)}",
            phone=faker.phone_number(),
            birth_date=faker.date_of_birth(minimum_age=18, maximum_age=85),
            is_active=random.choice([True, False]),
            notes=faker.sentence() if random.choice([True, False]) else None,
        )

        db.add(persona)

    db.commit()
    return cantidad

##Se crea la funcion reset_personas la cual elimina todos los registros de la tabla personas
def reset_personas(db: Session) -> int:
    """Eliminar todos los registros de la tabla personas.
        Retorna el número de registros eliminados. 
    """
    result = db.execute(text("DELETE FROM personas"))
    db.commit()
    return result.rowcount

## Se crea una funcion la cual retorna cuantas personas hay por dominio de correo
def estadisticas_por_dominio(db: Session) -> dict:
    """
    Retorna la cantidad de personas agrupadas por dominio de correo.
    """
    query = text("""
        SELECT
            SUBSTRING_INDEX(email, '@', -1) AS dominio,
            COUNT(*) AS total
        FROM personas
        GROUP BY dominio
    """)

    result = db.execute(query).fetchall()

    return {row.dominio: row.total for row in result} 

## Se crea la query la cual calcula estadisticas de la edad
def estadisticas_edad(db: Session) -> dict:
    """
    Retorna estadísticas de edad (min, max, promedio).
    """
    query = text("""
        SELECT
            MIN(TIMESTAMPDIFF(YEAR, birth_date, CURDATE())) AS edad_min,
            MAX(TIMESTAMPDIFF(YEAR, birth_date, CURDATE())) AS edad_max,
            ROUND(AVG(TIMESTAMPDIFF(YEAR, birth_date, CURDATE())), 1) AS edad_promedio
        FROM personas
        WHERE birth_date IS NOT NULL
    """)

    result = db.execute(query).first()

    return {
        "edad_minima": result.edad_min,
        "edad_maxima": result.edad_max,
        "edad_promedio": result.edad_promedio
    }


## Nueva funcion para buscar personas por nombre, apellido o email
def buscar_personas(db: Session, termino: str) -> Sequence[Persona]:
    """
    Busca personas por first_name, last_name o email.
    """
    patron = f"%{termino}%"

    return (
        db.query(Persona)
        .filter(
            or_(
                Persona.first_name.ilike(patron),
                Persona.last_name.ilike(patron),
                Persona.email.ilike(patron),
            )
        )
        .all()
    )

## se agrega la funcion reporte personas_activas
def reporte_personas_activas(db: Session) -> List[Dict]:
    """
    Retorna un reporte reducido de personas activas.
    """
    personas = (
        db.query(
            Persona.id,
            Persona.email,
            Persona.phone,
            Persona.is_active
        )
        .filter(Persona.is_active.is_(True))
        .all()
    )

    return [
        {
            "id": p.id,
            "email": p.email,
            "phone": p.phone,
            "is_active": p.is_active
        }
        for p in personas
    ]