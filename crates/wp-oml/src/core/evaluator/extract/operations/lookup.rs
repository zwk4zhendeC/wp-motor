use crate::core::FieldExtractor;
use crate::core::prelude::*;
use crate::language::LookupOperation;
use std::sync::Arc;
use wp_model_core::model::{DataField, DataRecord, FieldStorage, Value};

fn normalize_lookup_key(raw: &str) -> String {
    raw.trim().to_lowercase()
}

impl LookupOperation {
    fn resolve_storage(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<FieldStorage> {
        let key_field = self.key().extract_one(target, src, dst);
        if let Some(field) = key_field
            && let Value::Chars(raw) = field.get_value()
            && let Some(dict) = self.compiled()
            && let Some(found) = dict.get(&normalize_lookup_key(raw))
        {
            return Some(FieldStorage::from_shared(Arc::clone(found)));
        }

        self.default().extract_storage(target, src, dst)
    }
}

impl FieldExtractor for LookupOperation {
    fn extract_one(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<DataField> {
        self.resolve_storage(target, src, dst)
            .map(FieldStorage::into_owned)
    }

    fn extract_storage(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<FieldStorage> {
        self.resolve_storage(target, src, dst)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::DataTransformer;
    use crate::parser::oml_parse_raw;
    use orion_error::TestAssert;
    use wp_knowledge::cache::FieldQueryCache;
    use wp_model_core::model::{DataField, DataRecord, FieldStorage, Value};

    #[test]
    fn test_lookup_nocase_matches_static_object_keys() {
        let mut conf = r#"
name : test
---
static {
    status_score = object {
        error = float(90.0);
        warning = float(70.0);
    };
}

risk_score : float = lookup_nocase(status_score, read(status), 40.0);
"#;
        let model = oml_parse_raw(&mut conf).assert();
        let cache = &mut FieldQueryCache::default();
        let src = DataRecord::from(vec![FieldStorage::from_owned(DataField::from_chars(
            "status", " ERROR ",
        ))]);

        let target = model.transform(src, cache);
        let storage = target.field("risk_score").expect("risk_score field");
        assert_eq!(storage.get_name(), "risk_score");
        assert_eq!(storage.as_field().get_value(), &Value::Float(90.0));
    }

    #[test]
    fn test_lookup_nocase_falls_back_for_non_string_key() {
        let mut conf = r#"
name : test
---
static {
    status_score = object {
        error = float(90.0);
    };
}

risk_score : float = lookup_nocase(status_score, read(code), 40.0);
"#;
        let model = oml_parse_raw(&mut conf).assert();
        let cache = &mut FieldQueryCache::default();
        let src = DataRecord::from(vec![FieldStorage::from_owned(DataField::from_digit(
            "code", 500,
        ))]);

        let target = model.transform(src, cache);
        let storage = target.field("risk_score").expect("risk_score field");
        assert_eq!(storage.get_name(), "risk_score");
        assert_eq!(storage.as_field().get_value(), &Value::Float(40.0));
    }
}
