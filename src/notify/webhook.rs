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
        let method = method.parse::<Method>().unwrap_or(Method::POST);

        let mut header_map = HeaderMap::new();
        for (key, value) in &headers {
            if let (Ok(name), Ok(val)) = (key.parse::<HeaderName>(), HeaderValue::from_str(value)) {
                header_map.insert(name, val);
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
