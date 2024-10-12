from fastapi import FastAPI, Depends, HTTPException
from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Boolean, UniqueConstraint, DateTime, func
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.ext.declarative import declarative_base

app = FastAPI()

SQLALCHEMY_DATABASE_URL = "postgresql://postgres:Su%40010403@localhost/blaash"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class EngagementPost(Base):
    __tablename__ = 'engagement_post'
    
    engagement_post_id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(Integer, nullable=False)
    number_of_likes = Column(Integer, default=0)
    number_of_shares = Column(Integer, default=0)
    description = Column(String, nullable=True)
    created_by = Column(String, nullable=False)
    created_on = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_by = Column(String, nullable=True)
    updated_on = Column(DateTime, onupdate=datetime.utcnow, nullable=True)
    customer_interaction_date = Column(DateTime, nullable=True)
    shopping_url = Column(String, nullable=True)
    customers_who_liked = Column(String, nullable=True)
    content_type = Column(String, nullable=False)
    Inflencer_id = Column(Integer, nullable=False)
    tags = Column(String, nullable=True)
    thumbnail_url = Column(String, nullable=True)
    thumbnail_title = Column(String, nullable=True)
    is_cancelled = Column(Boolean, default=False)
    schedule_code = Column(String, nullable=True)
    button_cta = Column(String, nullable=True)
    is_new_collection = Column(Boolean, default=False)
    video_duration = Column(Integer, nullable=True)
    is_multihost = Column(Boolean, default=False)
    disabled_product = Column(Boolean, default=False)
    cta_url = Column(String, nullable=True)
    product_thumbnail_url = Column(String, nullable=True)

class EngagementPostContent(Base):
    __tablename__ = 'engagement_post_content'
    
    engagement_post_content_id = Column(Integer, primary_key=True, index=True)
    file_type = Column(String, nullable=False)
    story_id = Column(Integer, ForeignKey('stories.story_id'), nullable=False)
    url = Column(String, nullable=False)
    thumbnail_url = Column(String, nullable=True)
    sequence = Column(Integer, nullable=False)

class EngagementPostProductMapping(Base):
    __tablename__ = 'engagement_post_product_mapping'
    
    engagement_post_product_mapping_id = Column(Integer, primary_key=True, index=True)
    engagement_post_id = Column(Integer, ForeignKey('engagement_post.engagement_post_id'), nullable=False)
    product_id = Column(Integer, ForeignKey('engagement_post_product.product_id'), nullable=False)

class EngagementPostProduct(Base):
    __tablename__ = 'engagement_post_product'
    
    product_id = Column(Integer, primary_key=True, index=True)
    product_name = Column(String, nullable=False)
    product_image = Column(String, nullable=True)
    sku_number = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint('sku_number', name='uq_sku_number'),  
    )

class Collection(Base):
    __tablename__ = 'collection'
    
    collection_id = Column(Integer, primary_key=True, index=True)
    collection_name = Column(String, nullable=False)

class EngagementPostCollection(Base):
    __tablename__ = 'engagement_post_collection'
    
    engagement_post_collection_id = Column(Integer, primary_key=True, index=True)
    engagement_post_id = Column(Integer, ForeignKey('engagement_post.engagement_post_id'))
    collection_id = Column(Integer, ForeignKey('collection.collection_id'))
    duration_in_seconds = Column(Integer)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class EngagementPostProductSchema(BaseModel):
    product_name: str
    product_image: str
    sku_number: str

class CollectionSchema(BaseModel):
    collection_name: str

class EngagementPostCollectionSchema(BaseModel):
    engagement_post_id: int
    collection_id: int
    duration_in_seconds: int

class EngagementPostResponse(BaseModel):
    engagement_post_id: Optional[int]
    tenant_id: int
    description: Optional[str] = None
    created_by: Optional[str]
    created_on: datetime
    thumbnail_url: Optional[str] = None
    thumbnail_title: Optional[str] = None
    products: List[EngagementPostProductSchema] = []

class TopViewedPostResponse(BaseModel):
    thumbnail_title: Optional[str] = None
    content_url: Optional[str] = None

class TopViewedProductResponse(BaseModel):
    product_name: Optional[str] = None
    content_url: Optional[str] = None
    duration_watched: Optional[float] = None  # in hours


# API to fetch a list of posts along with their content and products for a given tenant_id
@app.get("/posts/{tenant_id}", response_model=List[EngagementPostResponse], tags=["Posts"])
def get_posts(tenant_id: int, db: Session = Depends(get_db)):
    posts = db.query(EngagementPost).filter(EngagementPost.tenant_id == tenant_id).all()
    result = []

    for post in posts:
        products = (
            db.query(EngagementPostProduct)
            .join(EngagementPostProductMapping)
            .filter(EngagementPostProductMapping.engagement_post_id == post.engagement_post_id)
            .all()
        )
        result.append(EngagementPostResponse(
            engagement_post_id=post.engagement_post_id,
            tenant_id=post.tenant_id,
            description=post.description,
            created_by=post.created_by,
            created_on=post.created_on,
            thumbnail_url=post.thumbnail_url,
            thumbnail_title=post.thumbnail_title,
            products=[EngagementPostProductSchema(
                product_name=product.product_name,
                product_image=product.product_image,
                sku_number=product.sku_number
            ) for product in products]
        ))
    
    return result

# API to create a new product
@app.post("/products/", response_model=EngagementPostProductSchema, tags=["Products"])
def create_product(product: EngagementPostProductSchema, db: Session = Depends(get_db)):
    db_product = EngagementPostProduct(**product.dict())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product

# API to create a new collection and save the post IDs in each collection
@app.post("/collections/", response_model=CollectionSchema, tags=["Collections"])
def create_collection(collection: CollectionSchema, post_ids: List[int], db: Session = Depends(get_db)):
    db_collection = Collection(**collection.dict())
    db.add(db_collection)
    db.commit()
    db.refresh(db_collection)
    
    for post_id in post_ids:
        mapping = EngagementPostCollection(engagement_post_id=post_id, collection_id=db_collection.collection_id)
        db.add(mapping)
    
    db.commit()
    
    return db_collection

# API to list the top 5 viewed engagement posts for a given tenant_id
@app.get("/top-viewed-posts/{tenant_id}", response_model=List[TopViewedPostResponse], tags=["Top Posts"])
def get_top_viewed_posts(tenant_id: int, db: Session = Depends(get_db)):
    posts = (
        db.query(EngagementPost)
        .filter(EngagementPost.tenant_id == tenant_id)
        .order_by(EngagementPost.number_of_shares.desc())  
        .all()
    )
    return [{"thumbnail_title": post.thumbnail_title, "content_url": post.shopping_url} for post in posts]

# API to list the top 5 products that are viewed most frequently for a given tenant_id
@app.get("/top-viewed-products/{tenant_id}", response_model=List[TopViewedProductResponse], tags=["Top Products"])
def get_top_viewed_products(tenant_id: int, db: Session = Depends(get_db)):
    product_views = (
        db.query(EngagementPostProductMapping.product_id, func.count(EngagementPostProductMapping.engagement_post_id).label("view_count"))
        .join(EngagementPost)
        .filter(EngagementPost.tenant_id == tenant_id)
        .group_by(EngagementPostProductMapping.product_id)
        .order_by(func.count(EngagementPostProductMapping.engagement_post_id).desc())
        .limit(5)
        .all()
    )
    
    result = []
    for product_id, view_count in product_views:
        product = db.query(EngagementPostProduct).filter(EngagementPostProduct.product_id == product_id).first()
        if product:
            duration_watched = view_count * (product.video_duration / 3600) if product.video_duration else 0
            result.append({
                "product_name": product.product_name,
                "content_url": product.shopping_url,  
                "duration_watched": duration_watched
            })

    return result
