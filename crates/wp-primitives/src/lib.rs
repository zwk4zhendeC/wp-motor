//! Shared parsing primitives.

pub use winnow::Parser;
pub type WResult<T> = winnow::ModalResult<T>;

pub mod atom;
pub mod comment;
pub mod fun;
pub mod net;
pub mod scope;
pub mod symbol;
pub mod utils;
