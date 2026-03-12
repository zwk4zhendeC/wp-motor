use crate::language::{LookupOperation, PreciseEvaluator};
use crate::parser::fun_prm::oml_gw_fun;
use crate::parser::pipe_prm;
use crate::parser::static_ctx::parse_static_value;
use crate::parser::tdc_prm::{oml_aga_tdc, oml_aga_value, oml_sql_raw};
use winnow::ascii::multispace0;
use winnow::combinator::{alt, trace};
use winnow::error::{ContextError, ErrMode, StrContext, StrContextValue};
use wp_parser::Parser;
use wp_parser::WResult;
use wp_parser::atom::take_var_name;
use wp_parser::symbol::{symbol_comma, symbol_semicolon};
use wp_parser::utils::get_scope;

pub fn oml_aga_lookup_nocase(data: &mut &str) -> WResult<PreciseEvaluator> {
    let op = trace("lookup_nocase", oml_lookup_nocase).parse_next(data)?;
    Ok(PreciseEvaluator::Lookup(op))
}

fn lookup_arg(data: &mut &str) -> WResult<PreciseEvaluator> {
    alt((
        oml_aga_lookup_nocase,
        pipe_prm::oml_aga_pipe_noprefix,
        oml_aga_tdc,
        oml_sql_raw,
        oml_aga_value,
        oml_gw_fun,
        parse_static_value,
    ))
    .parse_next(data)
}

pub fn oml_lookup_nocase(data: &mut &str) -> WResult<LookupOperation> {
    multispace0.parse_next(data)?;
    "lookup_nocase"
        .context(StrContext::Label("oml keyword"))
        .context(StrContext::Expected(StrContextValue::Description(
            "need 'lookup_nocase' keyword",
        )))
        .parse_next(data)?;

    let scope = get_scope(data, '(', ')')?;
    let mut args = scope;

    multispace0.parse_next(&mut args)?;
    let dict_symbol = take_var_name
        .context(StrContext::Label("lookup_nocase dict"))
        .parse_next(&mut args)?
        .to_string();
    multispace0.parse_next(&mut args)?;
    symbol_comma.parse_next(&mut args)?;

    let key = lookup_arg.parse_next(&mut args)?;
    multispace0.parse_next(&mut args)?;
    symbol_comma.parse_next(&mut args)?;

    let default = lookup_arg.parse_next(&mut args)?;
    multispace0.parse_next(&mut args)?;
    let _ = symbol_semicolon.parse_next(&mut args);
    multispace0.parse_next(&mut args)?;
    if !args.is_empty() {
        return Err(ErrMode::Backtrack(ContextError::new()));
    }

    Ok(LookupOperation::new(dict_symbol, key, default))
}

#[cfg(test)]
mod tests {
    use crate::parser::lookup_prm::oml_lookup_nocase;
    use crate::parser::utils::for_test::assert_oml_parse_ext;

    #[test]
    fn test_lookup_nocase_parse() {
        let mut code = r#"lookup_nocase(status_score, read(status), 40.0)"#;
        let expect = r#"lookup_nocase(status_score, read(status), 40)"#;
        assert_oml_parse_ext(&mut code, oml_lookup_nocase, expect);
    }
}
