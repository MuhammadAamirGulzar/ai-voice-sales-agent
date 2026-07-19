from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Float,
    Integer,
    JSON,
    String,
    Boolean,
    Integer,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    phone_number = Column(String(20), nullable=True)
    username = Column(String(100), unique=True, nullable=False)
    password = Column(String(100), nullable=False)
    organization_id = Column(Integer, ForeignKey("organizations.id"), nullable=True)

    organization = relationship("Organization", back_populates="users")


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String(1000), nullable=True)
    target_audience = Column(String(300), nullable=True)
    products = Column(JSON, nullable=True)
    other_details = Column(String(1000), nullable=True)

    users = relationship(
        "User", back_populates="organization", cascade="all, delete-orphan"
    )
    teams = relationship(
        "Team", back_populates="organization", cascade="all, delete-orphan"
    )
    chat_histories = relationship(
        "ChatHistory", back_populates="organization", cascade="all, delete-orphan"
    )


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    organization_id = Column(
        Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    campaign_goals = Column(String(500), nullable=True)
    selected_products = Column(JSON, nullable=True)

    organization = relationship("Organization", back_populates="teams")
    agents = relationship("Agent", back_populates="team", cascade="all, delete-orphan")
    chat_histories = relationship(
        "ChatHistory", back_populates="team", cascade="all, delete-orphan"
    )


class Agent(Base):
    __tablename__ = "agents"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    team_id = Column(
        Integer, ForeignKey("teams.id", ondelete="CASCADE"), nullable=False
    )
    agent_function = Column(String(1000), nullable=True)
    forwarding_criteria = Column(String(300), nullable=True)
    departments = Column(String(300), nullable=True)
    other_department = Column(String(300), nullable=True)
    use_elevenlabs = Column(Boolean, default=False)
    voice_id = Column(String(300), nullable=True)
    team = relationship("Team", back_populates="agents")
    chat_histories = relationship(
        "ChatHistory", back_populates="agent", cascade="all, delete-orphan"
    )


class ChatHistory(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, index=True)
    organization_id = Column(
        Integer, ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    team_id = Column(
        Integer, ForeignKey("teams.id", ondelete="CASCADE"), nullable=False
    )
    agent_id = Column(
        Integer, ForeignKey("agents.id", ondelete="CASCADE"), nullable=False
    )
    chat_data = Column(JSON, nullable=True)
    timestamp = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=True
    )
    response_time = Column(Float, nullable=True)
    # Per-turn latency waterfall from the streaming voice engine
    # (voice/metrics.py CallMetrics.to_dict()).
    metrics = Column(JSON, nullable=True)

    organization = relationship("Organization", back_populates="chat_histories")
    team = relationship("Team", back_populates="chat_histories")
    agent = relationship("Agent", back_populates="chat_histories")
