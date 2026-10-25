use crate::client::Model;

/// The model that answers a nested web search for an agent on `model`.
///
/// OpenAI's `web_search` tool runs only on OpenAI's models: through a Responses gateway another
/// vendor's model either is unknown under the OpenAI vendor prefix (HTTP 404) or ignores the tool
/// and answers without searching, so those agents search with `fallback` instead. OpenAI's own
/// models, and custom models without another vendor's prefix, search on the calling model.
pub(super) fn search_model(model: &str, fallback: Model) -> &str {
    let other_vendor = match Model::known(model) {
        Some(Model::Glm53 | Model::Kimi | Model::Mimo) => true,
        Some(_) => false,
        None => model
            .split_once('/')
            .is_some_and(|(vendor, _)| vendor != "openai"),
    };
    if other_vendor {
        fallback.as_str()
    } else {
        model
    }
}
