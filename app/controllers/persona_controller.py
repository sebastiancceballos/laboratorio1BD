from typing import List
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from ..database import get_db
from ..views.persona import PersonaCreate, PersonaUpdate, PersonaRead
from ..services import persona_service
## Se importa la funcion populate_personas de persona_service y la libreria HTTPException
from ..services.persona_service import populate_personas
from fastapi import HTTPException

## Se importa  esquema de los datos a retornar
from ..views.persona import poblarRequest

##Se importa la funcion reset_personas
from ..services.persona_service import reset_personas

##Se importa la funcion estadisticas_por_dominio
from ..services.persona_service import estadisticas_por_dominio


router = APIRouter(prefix="/personas", tags=["personas"])


@router.post("", response_model=PersonaRead, status_code=status.HTTP_201_CREATED)
def create_persona(persona_in: PersonaCreate, db: Session = Depends(get_db)):
    """Create a new Persona delegating to service layer."""
    # Let domain errors bubble up to global handlers
    return persona_service.create_persona(db, persona_in)


@router.get("", response_model=List[PersonaRead])
def list_personas(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """List Personas with pagination via service layer."""
    return persona_service.list_personas(db, skip=skip, limit=limit)


@router.get("/{persona_id}", response_model=PersonaRead)
def get_persona(persona_id: int, db: Session = Depends(get_db)):
    """Retrieve a Persona by ID via service layer."""
    return persona_service.get_persona(db, persona_id)


@router.put("/{persona_id}", response_model=PersonaRead)
def update_persona(persona_id: int, persona_in: PersonaUpdate, db: Session = Depends(get_db)):
    """Update an existing Persona (partial) via service layer."""
    return persona_service.update_persona(db, persona_id, persona_in)



## Nueva ruta para resetear personas
@router.delete("/reset")
def borrar_todas_las_personas(db: Session = Depends(get_db)):
    total = reset_personas(db)
    return {
        "mensaje": "Base de datos limpiada. Se eliminaron todos los registros.",
        "total_eliminadas": total
    }

@router.delete("/{persona_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_persona(persona_id: int, db: Session = Depends(get_db)):
    """Delete a Persona by ID via service layer."""
    persona_service.delete_persona(db, persona_id)
    return None


## Nueva ruta para poblar personas
@router.post("/poblar", status_code=201)
def poblar_personas(payload: poblarRequest, db: Session = Depends(get_db)):
    total = populate_personas(db, payload.cantidad)
    return {"message": f"{total} personas creadas exitosamente"}

## Nueva ruta para obtener estadisticas por dominio
@router.get("/estadisticas/dominios")
def estadisticas_dominios(db: Session = Depends(get_db)):
    """
    Retorna estadísticas de personas por dominio de correo.
    """
    return estadisticas_por_dominio(db)


