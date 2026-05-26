from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Any]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    
    drift_severity = 'NONE'
    if new_columns:
        if any(actual_schema[col] not in ['string', 'float', 'boolean'] or actual_schema[col]!='string' for col in new_columns):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    
    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    decisions = {}
    for col, dtype in drift_report['new_columns'].items():
        if dtype == 'string':
            decisions[col] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable string column', 'risk_level': 'LOW'}
        elif dtype in ['float', 'numeric']:
            decisions[col] = {'action': 'FLAG_ANOMALY','reason': 'New float/numeric column', 'risk_level': 'HIGH'}
        elif dtype == 'boolean':
            decisions[col] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable boolean column', 'risk_level': 'LOW'}
    
    for col in drift_report['removed_columns']:
        decisions[col] = {'action': 'HALT','reason': 'Removed column', 'risk_level': 'BREAKING'}
    
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, str]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for col, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(col)
        elif decision['action'] == 'ADD_TO_SCHEMA':
            migration_notes.append(f"Added new column: {col} with type {updated_schema[col]}")
        elif decision['action'] == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f"{col}_anomaly_flag", spark_df[col].isNull().cast("boolean"))
            migration_notes.append(f"Flagged anomaly for column: {col}")
        elif decision['action'] == 'HALT':
            raise ValueError(f"Schema drift would break consumers: {decision['reason']}")
    
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Any]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    
    print("Schema Drift Report:")
    print(f"New Columns: {drift_report['new_columns']}")
    print(f"Removed Columns: {drift_report['removed_columns']}")
    print(f"Type Changes: {drift_report['type_changes']}")
    print(f"Drift Severity: {drift_report['drift_severity']}")
    
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        return {
            "drift_report": drift_report,
            "decisions": decisions,
            "migration_notes": migration_notes,
            "evolved_df": evolved_df
        }
    
    return {
        "drift_report": drift_report,
        "decisions": decisions
    }
