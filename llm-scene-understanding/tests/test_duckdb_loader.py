import pytest
import json
from pathlib import Path
from scene_understanding.duckdb_loader import FashionpediaDuckDBLoader

@pytest.fixture(scope="module")
def test_data(tmp_path_factory):
    json_path = Path("data/01_raw/instances_attributes_train2020.json")
    assert json_path.exists(), f"JSON file not found: {json_path}"
    with open(json_path, "r") as f:
        data = json.load(f)
    # Prepare subset as in test_duckdb.py
    licenses = data.get('licenses', [])[:2]
    license_ids = {l['id'] for l in licenses}
    images = [img for img in data.get('images', [])[:10] if img.get('license') in license_ids]
    image_ids = {img['id'] for img in images}
    categories = data.get('categories', [])[:5]
    category_ids = {c['id'] for c in categories}
    attributes = data.get('attributes', [])[:10]
    attribute_ids = {a['id'] for a in attributes}
    annotations = [a for a in data.get('annotations', [])[:100] if a.get('image_id') in image_ids and a.get('category_id') in category_ids]
    return {
        'info': data.get('info', {}),
        'licenses': licenses,
        'categories': categories,
        'attributes': attributes,
        'images': images,
        'annotations': annotations
    }

def test_duckdb_load_and_verify(test_data, tmp_path):
    schema_path = Path("data/01_raw/fashionpedia_schema.sql")
    db_path = tmp_path / "fashionpedia_test.ddb"
    # Load data
    with FashionpediaDuckDBLoader(db_path) as loader:
        loader.create_schema(schema_path)
        loader._load_info(test_data['info'])
        loader._load_licenses(test_data['licenses'])
        loader._load_categories(test_data['categories'])
        loader._load_attributes(test_data['attributes'])
        loader._load_images(test_data['images'])
        loader._load_annotations(test_data['annotations'])
        # Verify images
        db_images = loader.connection.execute("SELECT id, width, height, file_name, license FROM images").fetchall()
        assert len(db_images) == len(test_data['images'])
        db_image_ids = {row[0] for row in db_images}
        for img in test_data['images']:
            assert img['id'] in db_image_ids
        # Verify categories
        db_categories = loader.connection.execute("SELECT id, name FROM categories").fetchall()
        assert len(db_categories) == len(test_data['categories'])
        db_category_ids = {row[0] for row in db_categories}
        for cat in test_data['categories']:
            assert cat['id'] in db_category_ids
        # Verify attributes
        db_attributes = loader.connection.execute("SELECT id, name FROM attributes").fetchall()
        assert len(db_attributes) == len(test_data['attributes'])
        db_attribute_ids = {row[0] for row in db_attributes}
        for attr in test_data['attributes']:
            assert attr['id'] in db_attribute_ids
        # Verify annotations
        db_annotations = loader.connection.execute("SELECT id, image_id, category_id FROM annotations").fetchall()
        # Only those with valid image/category are inserted
        expected_ann_ids = {a['id'] for a in test_data['annotations']}
        db_ann_ids = {row[0] for row in db_annotations}
        assert db_ann_ids.issubset(expected_ann_ids)
        # Optionally, check that all inserted annotations have valid image/category
        for row in db_annotations:
            assert row[1] in db_image_ids
            assert row[2] in db_category_ids 