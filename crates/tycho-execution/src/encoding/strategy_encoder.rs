use crate::encoding::{
    errors::EncodingError,
    models::{EncodedSolution, Solution},
};

/// A trait that defines how to encode a `Solution` for execution.
pub(crate) trait StrategyEncoder {
    /// `encode_strategy` takes a `Solution`, which contains all the necessary information about
    /// the swaps to be performed, and encodes it into a format that can be executed by the router
    /// or executor contracts.
    ///
    /// # Arguments
    /// * `solution` - The `Solution` to encode, containing swap details, amounts, and execution
    ///   path
    ///
    /// # Returns
    /// * `Result<EncodedSwaps, EncodingError>`
    fn encode_strategy(&self, solution: &Solution) -> Result<EncodedSolution, EncodingError>;
}
