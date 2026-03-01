// BatchInference — auto-batching for GPU inference

use tl_ai::{TlModel, TlTensor, predict_batch};

/// Batch inference helper. Currently delegates to CPU-based predict_batch.
/// Future: keep intermediate data on GPU between batches.
pub struct BatchInference;

impl BatchInference {
    /// Run batched prediction. Splits input into batches and collects results.
    pub fn batch_predict(
        model: &TlModel,
        input: &TlTensor,
        batch_size: Option<usize>,
    ) -> Result<TlTensor, String> {
        let bs = batch_size.unwrap_or(32);
        predict_batch(model, input, bs)
    }
}

#[cfg(test)]
mod tests {
    // Batch inference tests require a model file, tested at integration level
}
