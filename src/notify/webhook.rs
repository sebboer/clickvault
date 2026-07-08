use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use super::{BackupEvent, Notifier};
use crate::error::ClickVaultError;
use crate::retry::RetryPolicy;

pub struct WebhookNotifier {
    url: String,
    method: Method,
    headers: HeaderMap,
    client: reqwest::Client,
    retry: RetryPolicy,
}

impl WebhookNotifier {
    pub fn new(
        url: String,
        method: String,
        headers: HashMap<String, String>,
        client: reqwest::Client,
        retry: RetryPolicy,
    ) -> Self {
        let method = method.parse::<Method>().unwrap_or_else(|_| {
            tracing::warn!(method = %method, "Invalid webhook HTTP method; falling back to POST");
            Method::POST
        });

        let mut header_map = HeaderMap::new();
        for (key, value) in &headers {
            match (key.parse::<HeaderName>(), HeaderValue::from_str(value)) {
                (Ok(name), Ok(val)) => {
                    header_map.insert(name, val);
                }
                _ => tracing::warn!(header = %key, "Ignoring invalid webhook header"),
            }
        }

        Self {
            url,
            method,
            headers: header_map,
            client,
            retry,
        }
    }
}

#[async_trait]
impl Notifier for WebhookNotifier {
    async fn send(&self, event: &BackupEvent) -> Result<(), ClickVaultError> {
        let request = self
            .client
            .request(self.method.clone(), &self.url)
            .headers(self.headers.clone())
            .json(event);
        super::send_with_retry(&self.retry, request, "Webhook").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::RetryPolicy;

    fn build(method: &str, headers: HashMap<String, String>) -> WebhookNotifier {
        WebhookNotifier::new(
            "https://example.com/hook".into(),
            method.into(),
            headers,
            reqwest::Client::new(),
            RetryPolicy::default(),
        )
    }

    #[test]
    fn parses_known_method_and_falls_back_to_post() {
        assert_eq!(build("PUT", HashMap::new()).method, Method::PUT);
        assert_eq!(build("not a method!", HashMap::new()).method, Method::POST);
    }

    #[test]
    fn keeps_valid_headers_and_drops_invalid_ones() {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer t".to_string());
        headers.insert("bad header name".to_string(), "x".to_string());
        headers.insert("X-Ok".to_string(), "value\nwith newline".to_string());

        let notifier = build("POST", headers);
        assert_eq!(notifier.headers.len(), 1);
        assert_eq!(notifier.headers["authorization"], "Bearer t");
    }
}
