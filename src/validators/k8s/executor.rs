use futures_util::io::AsyncBufReadExt;
use futures_util::TryStreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Pod, Service};
use kube::api::{Api, ListParams, LogParams, Patch, PatchParams};
use kube::config::{Config, KubeConfigOptions, Kubeconfig};
use kube::{Client, ResourceExt};

pub struct KubernetesExecutor {
    client: Client,
    namespace: Option<String>,
}

impl KubernetesExecutor {
    pub async fn new(
        kubeconfig_path: Option<String>,
        namespace: Option<String>,
    ) -> Result<Self, kube::Error> {
        let client = build_client(kubeconfig_path).await?;
        Ok(Self { client, namespace })
    }

    fn api<K>(&self) -> Api<K>
    where
        K: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>,
        K::DynamicType: Default,
    {
        match &self.namespace {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::default_namespaced(self.client.clone()),
        }
    }
    // Get all the pods
    pub async fn list_pods(&self) -> Result<(), kube::Error> {
        let pods = self.api::<Pod>();
        println!("Fetching pods...");

        for pod in pods.list(&ListParams::default()).await? {
            println!("{}", pod.name_any());
        }

        Ok(())
    }
    // Get all the deployments
    pub async fn list_deployments(&self) -> Result<(), kube::Error> {
        let deployments = self.api::<Deployment>();
        println!("Fetching deployments...");

        for d in deployments.list(&ListParams::default()).await? {
            println!("{}", d.name_any());
        }

        Ok(())
    }

    // Get all the services
    pub async fn list_svc(&self) -> Result<(), kube::Error> {
        let services = self.api::<Service>();
        println!("Fetching services...");

        for s in services.list(&ListParams::default()).await? {
            println!("{}", s.name_any());
        }

        Ok(())
    }

    // Scale a deployment
    pub async fn scale(&self, name: &str, replicas: i32) -> Result<(), kube::Error> {
        let deployments = self.api::<Deployment>();

        let patch = serde_json::json!({
            "spec": { "replicas": replicas }
        });

        deployments
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;

        println!("Deployment '{}' scaled to {} replicas", name, replicas);
        Ok(())
    }

    // Get pods log
    pub async fn logs_pod(&self, pod_name: &str, follow: bool) -> Result<(), kube::Error> {
        let pods = self.api::<Pod>();

        println!("Fetching logs for pod '{}'...", pod_name);

        if follow {
            let params = LogParams {
                follow: true,
                ..Default::default()
            };

            let mut stream = pods.log_stream(pod_name, &params).await?.lines();

            while let Some(line) = stream
                .try_next()
                .await
                .map_err(|e| kube::Error::Service(Box::new(e)))?
            {
                println!("{}", line);
            }
        } else {
            let logs = pods.logs(pod_name, &LogParams::default()).await?;
            println!("{}", logs);
        }

        Ok(())
    }

    // Describe a resource
    pub async fn describe_pod(&self, name: &str) -> Result<(), kube::Error> {
        let pods = self.api::<Pod>();
        let pod = pods.get(name).await?;

        println!("Describing pod '{}'...\n", name);
        let yaml =
            serde_yaml::to_string(&pod).unwrap_or_else(|e| format!("serialization error: {}", e));
        println!("{}", yaml);

        Ok(())
    }

    pub async fn describe_deployment(&self, name: &str) -> Result<(), kube::Error> {
        let deployments = self.api::<Deployment>();
        let deployment = deployments.get(name).await?;

        println!("Describing deployment '{}'...\n", name);
        let yaml = serde_yaml::to_string(&deployment)
            .unwrap_or_else(|e| format!("serialization error: {}", e));
        println!("{}", yaml);

        Ok(())
    }

    pub async fn describe_svc(&self, name: &str) -> Result<(), kube::Error> {
        let services = self.api::<Service>();
        let service = services.get(name).await?;

        println!("Describing service '{}'...\n", name);
        let yaml = serde_yaml::to_string(&service)
            .unwrap_or_else(|e| format!("serialization error: {}", e));
        println!("{}", yaml);

        Ok(())
    }
}

async fn build_client(kubeconfig_path: Option<String>) -> Result<Client, kube::Error> {
    match kubeconfig_path {
        Some(path) => client_from_path(&path).await,
        None => Client::try_default().await,
    }
}

async fn client_from_path(path: &str) -> Result<Client, kube::Error> {
    let kubeconfig = Kubeconfig::read_from(path)?;
    let config = Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default()).await?;
    Client::try_from(config)
}
