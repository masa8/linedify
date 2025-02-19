from google.cloud.firestore_v1.base_query import FieldFilter
from datetime import datetime, timezone
from typing import List
from google.cloud import firestore
import asyncio

class ConversationSession:
    def __init__(self, user_id: str, conversation_id: str = None, updated_at: datetime = None) -> None:
        self.user_id = user_id
        self.conversation_id = conversation_id
        self.updated_at = updated_at or datetime.now(timezone.utc)

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "conversation_id": self.conversation_id,
            "updated_at": self.updated_at.isoformat()
        }

    @staticmethod
    def from_dict(data):
        return ConversationSession(
            user_id=data["user_id"],
            conversation_id=data.get("conversation_id"),
            updated_at=datetime.fromisoformat(data["updated_at"])
        )

class ConversationSessionStore:
    def __init__(self) -> None:
        self.db = firestore.AsyncClient()  # 非同期 Firestore クライアント
        self.collection = self.db.collection("conversation_sessions")  # Firestore のコレクション名

    async def get_session(self, user_id: str) -> ConversationSession:
        if not user_id:
            raise ValueError("user_id is required")

        try:
            query = (
                self.collection
                .where(filter=FieldFilter(field_path="user_id", op_string="==", value=user_id))
                .limit(1)
            )
    
            docs = await query.get()
    
            if not docs:
                return ConversationSession(user_id)
    
            db_session = docs[0].to_dict()
            session_obj = ConversationSession.from_dict(db_session)
            return session_obj
            
        except Exception as e:
            raise RuntimeError(f"Error fetching session for user_id={user_id}") from e
            
    async def set_session(self, session: ConversationSession) -> None:
        if not session.user_id:
            raise ValueError("user_id is required")

        # Check if user_id already exists
        query = self.collection.where(filter=FieldFilter(field_path="user_id", op_string="==", value=session.user_id)).limit(1)
        docs = await query.get()

        # If user_id exists, do nothing
        if docs:
            return  # Simply exit
            
        session.updated_at = datetime.now(timezone.utc)
        doc_ref = self.collection.document(f"{session.user_id}_{session.conversation_id}")
        await doc_ref.set(session.to_dict())

    async def expire_session(self, user_id: str) -> None:
        if not user_id:
            raise ValueError("user_id is required")

        query = (
            self.collection
            .where(filter=FieldFilter(field_path="user_id", op_string="==", value=user_id))
            .order_by("updated_at", direction=firestore.Query.DESCENDING)
            .limit(1)
        )

        docs = await query.get()
        if docs:
            doc_ref = docs[0].reference
            await doc_ref.update({"is_expired": True})

    async def get_user_conversations(self, user_id: str, count: int = 20) -> List[ConversationSession]:
        query = (
            self.collection
            .where(filter=FieldFilter(field_path="user_id", op_string="==", value=user_id))
            .order_by("updated_at", direction=firestore.Query.DESCENDING)
            .limit(count)
        )

        docs = await query.get()
        return [ConversationSession.from_dict(doc.to_dict()) for doc in reversed(docs)]

