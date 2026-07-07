use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use super::{BackupEvent, Notifier};
use crate::error::ClickVaultError;

pub struct WebhookNotifier {
    url: String,
    method: Method,
    headers: HeaderMap,
    client: reqwest::Client,
}

impl WebhookNotifier {
    pub fn new(
        url: String,
        method: String,
        headers: HashMap<String, String>,
        client: reqwest::Client,
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
        }
    }
}

#[async_trait]
impl Notifier for WebhookNotifier {
    async fn send(&self, event: &BackupEvent) -> Result<(), ClickVaultError> {
        let response = self
            .client
            .request(self.method.clone(), &self.url)
            .headers(self.headers.clone())
            .json(event)
            .send()
            .await
            .map_err(|e| ClickVaultError::Notification(format!("Webhook request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ClickVaultError::Notification(format!(
                "Webhook returned {status}: {body}"
            )));
        }

        Ok(())
    }
}
