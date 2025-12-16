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