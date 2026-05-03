<a id="submarine-apache-org-docs-gettingstarted-quickstart-index"></a>

# Quickstart | Apache Submarine

- [🏠](/)
- Getting Started
- Quickstart
Version: 0.8.0

On this page

# Quickstart

This document gives you a quick view on the basic usage of Submarine platform. You can finish each step of ML model lifecycle on the platform without messing up with the troublesome environment problems.

## Installation

### Prepare a Kubernetes cluster

1. Prerequisite

- Check [dependency page](/docs/devDocs/Dependencies) for the compatible version
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [helm](https://helm.sh/docs/intro/install/) (Helm v3 is minimum requirement.)
- [minikube](https://minikube.sigs.k8s.io/docs/start/).
- [istioctl](https://istio.io/latest/docs/setup/getting-started/#download)

2. Start minikube cluster and install Istio

Start minikube

```bash
# You can go to https://minikube.sigs.k8s.io/docs/start/ and follow the tutorial to install minikube.# Then you can start kubernetes with minikube:minikube start --vm-driver=docker --cpus 8 --memory 8192 --kubernetes-version v1.24.12# The version of k8s can be adjusted to the range of your current minikube. # For example, minikube v1.28.0 can provide versions from v1.25.0 to v1.25.3 in k8s 1.25# Or if you want to support Pod Security Policy (https://minikube.sigs.k8s.io/docs/tutorials/using_psp) in k8s 1.21 or 1.22, you can use the following command to start clusterminikube start --extra-config=apiserver.enable-admission-plugins=PodSecurityPolicy --addons=pod-security-policy --vm-driver=docker --cpus 8 --memory 8192 --kubernetes-version v1.21.2
```

Install Istio, there are two ways to install: Command-Istioctl-based, or Helm-based

```bash
# You can go to the https://github.com/istio/istio/releases/ to download the istioctl for your k8s version# e.g. we can execute the following command to download the istio version adapted to k8s 1.24.12# wget https://github.com/istio/istio/releases/download/1.17.1/istio-1.17.1-linux-amd64.tar.gzistioctl install -y# Alternatively, you can use istio's helm to install# This is the link: https://istio.io/latest/docs/setup/install/helm/## Add istio repohelm repo add istio https://istio-release.storage.googleapis.com/chartshelm repo update## Create istio-system namespacekubectl create namespace istio-system## Install istio resourceshelm install istio-base istio/base -n istio-systemhelm install istiod istio/istiod -n istio-systemhelm install istio-ingressgateway istio/gateway -n istio-system
```

### Launch submarine in the cluster

1. Clone the project

```bash
git clone https://github.com/apache/submarine.gitcd submarine
```

2. Create necessary namespaces

```bash
# create namespace for submarine, training, notebook and seldon-core operatorskubectl create namespace submarinekubectl label namespace submarine istio-injection=enabled# create namespace for deploying submarine-serverkubectl create namespace submarine-user-testkubectl label namespace submarine-user-test istio-injection=enabled# After k8s 1.25, we can turn on PSA (Pod Security Admission) labels for namespace.# We use a common PSA enforcement level. If you want to use a more detailed configuration, you can refer to# https://kubernetes.io/docs/concepts/security/pod-security-admission/#pod-security-admission-labels-for-namespaceskubectl label namespace submarine-user-test 'pod-security.kubernetes.io/enforce=privileged'
```

3. Install the submarine operator and dependencies by helm chart

```bash
# Update helm dependency.helm dependency update ./helm-charts/submarine# Install submarine operator in namespace submarine.helm install submarine ./helm-charts/submarine --set seldon-core-operator.istio.gateway=submarine/seldon-gateway -n submarine
```

4. Create a Submarine custom resource and the operator will create the submarine server, database, etc. for us.

```bash
kubectl apply -f submarine-cloud-v3/config/samples/_v1_submarine.yaml -n submarine-user-test
```

### Ensure submarine is ready

```bash
$ kubectl get pods -n submarineNAME                                              READY   STATUS    RESTARTS   AGEnotebook-controller-deployment-66d85984bf-x562z   1/1     Running   0          7h7mtraining-operator-6dcd5b9c64-nxwr2                1/1     Running   0          7h7msubmarine-operator-9cb7bc84d-brddz                1/1     Running   0          7h7m$ kubectl get pods -n submarine-user-testNAME                                     READY   STATUS    RESTARTS   AGEsubmarine-database-0                     1/1     Running   0          7h6msubmarine-minio-686b8777ff-zg4d2         2/2     Running   0          7h6msubmarine-mlflow-68c5559dcb-lkq4g        2/2     Running   0          7h6msubmarine-server-7c6d7bcfd8-5p42w        2/2     Running   0          9m33ssubmarine-tensorboard-57c5b64778-t4lww   2/2     Running   0          7h6m
```

### Connect to workbench

1. Exposing service

```bash
kubectl port-forward --address 0.0.0.0 -n istio-system service/istio-ingressgateway 32080:80
```

2. View workbench

Go to `http://0.0.0.0:32080`
![](submarine.apache.org/assets/images/quickstart-worbench-0d8c2f6217f22460d4cf8e9b05d06f6b.png)

## Example: Submit a mnist distributed example

We put the code of this example [here](https://github.com/apache/submarine/tree/master/dev-support/examples/quickstart). `train.py` is our training script, and `build.sh` is the script to build a docker image.

### 1. Write a python script for distributed training

Take a simple mnist tensorflow script as an example. We choose `MultiWorkerMirroredStrategy` as our distributed strategy.

```python
"""./dev-support/examples/quickstart/train.pyReference: https://github.com/kubeflow/training-operator/blob/master/examples/tensorflow/distribution_strategy/keras-API/multi_worker_strategy-with-keras.py"""import tensorflow as tfimport tensorflow_datasets as tfdsfrom packaging.version import Versionfrom tensorflow.keras import layers, modelsimport submarinedef make_datasets_unbatched():    BUFFER_SIZE = 10000    # Scaling MNIST data from (0, 255] to (0., 1.]    def scale(image, label):        image = tf.cast(image, tf.float32)        image /= 255        return image, label    # If we use tensorflow_datasets > 3.1.0, we need to disable GCS    # https://github.com/tensorflow/datasets/issues/2761#issuecomment-1187413141    if Version(tfds.__version__) > Version("3.1.0"):        tfds.core.utils.gcs_utils._is_gcs_disabled = True    datasets, _ = tfds.load(name="mnist", with_info=True, as_supervised=True)    return datasets["train"].map(scale).cache().shuffle(BUFFER_SIZE)def build_and_compile_cnn_model():    model = models.Sequential()    model.add(layers.Conv2D(32, (3, 3), activation="relu", input_shape=(28, 28, 1)))    model.add(layers.MaxPooling2D((2, 2)))    model.add(layers.Conv2D(64, (3, 3), activation="relu"))    model.add(layers.MaxPooling2D((2, 2)))    model.add(layers.Conv2D(64, (3, 3), activation="relu"))    model.add(layers.Flatten())    model.add(layers.Dense(64, activation="relu"))    model.add(layers.Dense(10, activation="softmax"))    model.summary()    model.compile(optimizer="adam", loss="sparse_categorical_crossentropy", metrics=["accuracy"])    return modeldef main():    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy(        communication=tf.distribute.experimental.CollectiveCommunication.AUTO    )    BATCH_SIZE_PER_REPLICA = 4    BATCH_SIZE = BATCH_SIZE_PER_REPLICA * strategy.num_replicas_in_sync    with strategy.scope():        ds_train = make_datasets_unbatched().batch(BATCH_SIZE).repeat()        options = tf.data.Options()        options.experimental_distribute.auto_shard_policy = (            tf.data.experimental.AutoShardPolicy.DATA        )        ds_train = ds_train.with_options(options)        # Model building/compiling need to be within `strategy.scope()`.        multi_worker_model = build_and_compile_cnn_model()    class MyCallback(tf.keras.callbacks.Callback):        def on_epoch_end(self, epoch, logs=None):            # monitor the loss and accuracy            print(logs)            submarine.log_metric("loss", logs["loss"], epoch)            submarine.log_metric("accuracy", logs["accuracy"], epoch)    multi_worker_model.fit(ds_train, epochs=10, steps_per_epoch=70, callbacks=[MyCallback()])    # save model    submarine.save_model(multi_worker_model, "tensorflow")if __name__ == "__main__":    main()
```

### 2. Prepare an environment compatible with the training

Build a docker image equipped with the requirement of the environment.

```bash
eval $(minikube docker-env)./dev-support/examples/quickstart/build.sh
```

### 3. Submit the experiment

1. Open submarine workbench and click `+ New Experiment`
2. Choose `Define your experiment`
3. Fill the form accordingly. Here we set 3 workers.

   1. Step 1
      ![](submarine.apache.org/assets/images/quickstart-submit-1-0-7-0-cec455a03933cc7b038a35a141a743b9.png)
   2. Step 2
      ![](submarine.apache.org/assets/images/quickstart-submit-2-0-7-0-2bce3b75c9f7c0ee0f44ee9b2bdb742e.png)
   3. Step 3
      ![](submarine.apache.org/assets/images/quickstart-submit-3-0-7-0-f7f3107669746b2c2a58e0794051a24b.png)
   4. The experiment is successfully submitted
      ![](submarine.apache.org/assets/images/quickstart-submit-4-0-7-0-34946a5790013de952eb32d1246f4a23.png)
4. In the meantime, we have built this image in docker hub and you can run this experiment directly if you choose `quickstart` in `From predefined experiment library`.

### 4. Monitor the process

1. In our code, we use `submarine` from `submarine-sdk` to record the metrics. To see the result, click corresponding experiment with name `mnist-example` in the workbench.
2. To see the metrics of each worker, you can select a worker from the left top list.

![](submarine.apache.org/assets/images/quickstart-ui-0-7-0-821f5ad73116d9a9d3088cddcb576836.png)

### 5. Serve the model

1. Before serving, we need to register a new model.

![](submarine.apache.org/assets/images/submarine-register-model-6540e8f76744c0f1e84013e7e6d78341.png)

2. And then, check the output model in experiment page.

![](submarine.apache.org/assets/images/quickstart-artifacts-31b0ede0bd1f74695a266b185c70c1f1.png)

3. Click the button and register the model.

![](submarine.apache.org/assets/images/submarine-register-model-6540e8f76744c0f1e84013e7e6d78341.png)

4. Go to the model page and deploy our model for serving.

![](submarine.apache.org/assets/images/submarine-serve-model-37ce5e2709d59223d7a7461ecd4f90d1.png)

5. We can run the following commands to get the `VirtualService` and `Endpoint` that use istio for external port forward or ingress.

```bash
## get VirtualService with your model namekubectl describe VirtualService -n submarine-user-test -l model-name=tf-mnistName:         submarine-model-1-2508dd65692740b18ff5c6c6c162b863Namespace:    submarine-user-testLabels:       model-id=2508dd65692740b18ff5c6c6c162b863              model-name=tf-mnist              model-version=1Annotations:  <none>API Version:  networking.istio.io/v1beta1Kind:         VirtualServiceMetadata:  Creation Timestamp:  2022-09-18T05:26:38Z  Generation:          1  Managed Fields:    ...Spec:  Gateways:    submarine/seldon-gateway  Hosts:    *  Http:    Match:      Uri:        Prefix:  /seldon/submarine-user-test/1/1/    Rewrite:      Uri:  /    Route:      Destination:        Host:  submarine-model-1-2508dd65692740b18ff5c6c6c162b863        Port:          Number:  8000Events:            <none>
```

To confirm that the serving endpoint is available, try using the swagger address to confirm the availability of the interface.
In our example, the address of the swagger is: http://localhost:32080/seldon/submarine-user-test/1/1/api/v1.0/doc/

More details can be found in the official seldon documentation: [https://docs.seldon.io/projects/seldon-core/en/latest/workflow/serving.html#generated-documentation-swagger-ui](https://docs.seldon.io/projects/seldon-core/en/latest/workflow/serving.html#generated-documentation-swagger-ui)

6. After successfully serving the model, we can test the results of serving using the test python code [serve\_predictions.py](https://github.com/apache/submarine/blob/master/dev-support/examples/quickstart/serve_predictions.py)

![](submarine.apache.org/assets/images/submarine-serve-prediction-c6038a5bfd49f11d234ba11442618067.png)

[Edit this page](https://github.com/apache/submarine/edit/master/website/versioned_docs/version-0.8.0/gettingStarted/quickstart.md)

---

<a id="submarine-apache-org-docs-gettingstarted-helm-index"></a>

# Custom Configuation | Apache Submarine

- [🏠](/)
- Getting Started
- Custom Configuation
Version: 0.8.0

On this page

# Custom Configuation

## Helm Chart Volume Type

Submarine can support various [volume types](https://kubernetes.io/docs/concepts/storage/volumes/#nfs), currently including hostPath (default) and NFS. It can be easily configured in the `./helm-charts/submarine/values.yaml`, or you can override the default values in `values.yaml` by [helm CLI](https://helm.sh/docs/helm/helm_install/).

#### hostPath

- In hostPath, you can store data directly in your node.
- Usage:
  1. Configure setting in `./helm-charts/submarine/values.yaml`.
  2. To enable hostPath storage, set `.storage.type` to `host`.
  3. To set the root path for your storage, set `.storage.host.root` to `<any-path>`
- Example:

  ```yaml
  # ./helm-charts/submarine/values.yamlstorage:  type: host  host:    root: /tmp
  ```

#### NFS (Network File System)

- In NFS, it allows multiple clients to access a shared space.
- Prerequisite:
  1. A pre-existing NFS server. You have two options.
     1. Create NFS server

        ```bash
        kubectl create -f ./dev-support/nfs-server/nfs-server.yaml
        ```

        It will create a nfs-server pod in kubernetes cluster, and expose nfs-server ip at `10.96.0.2`
     2. Use your own NFS server
  2. Install NFS dependencies in your nodes
     - Ubuntu

       ```bash
       apt-get install -y nfs-common
       ```
     - CentOS

       ```bash
       yum install nfs-util
       ```
- Usage:
  1. Configure setting in `./helm-charts/submarine/values.yaml`.
  2. To enable NFS storage, set `.storage.type` to `nfs`.
  3. To set the ip for NFS server, set `.storage.nfs.ip` to `<any-ip>`
- Example:

  ```yaml
  # ./helm-charts/submarine/values.yamlstorage:  type: nfs  nfs:    ip: 10.96.0.2
  ```

### Access to Submarine Server

Submarine server by default expose 8080 port within K8s cluster. After Submarine v0.5
uses Traefik as reverse-proxy by default. If you don't want to
use Traefik, you can modify below value to ***false*** in `./helm-charts/submarine/values.yaml`.

```yaml
# Use Traefik by defaulttraefik:  enabled: true
```

To access the server from outside of the cluster, we use Traefik ingress controller and
NodePort for external access.\
Please refer to `./helm-charts/submarine/charts/traefik/values.yaml` and [Traefik docs](https://docs.traefik.io/)
for more details if you want to customize the default value for Traefik.

*Notice:*
If you use `kind` to run local Kubernetes cluster,
please refer to this [docs](https://kind.sigs.k8s.io/docs/user/configuration/#extra-port-mappings)
and set the configuration "extraPortMappings" when creating the k8s cluster.

```text
kind: ClusterapiVersion: kind.x-k8s.io/v1alpha4nodes:- role: control-plane  extraPortMappings:  - containerPort: 32080    hostPort: [the port you want to access]
```

```text
# Use nodePort and Traefik ingress controller by default.# To access the submarine server, open the following URL in your browser.http://127.0.0.1:32080
```

If minikube is installed, use the following command to find the URL to the Submarine server.

```text
$ minikube service submarine-traefik --url
```

## Kubernetes Dashboard (optional)

### Deploy

To deploy Dashboard, execute the following command:

```text
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.0.0-beta8/aio/deploy/recommended.yaml
```

### Create RBAC

Run the following commands to grant the cluster access permission of dashboard:

```text
kubectl create serviceaccount dashboard-admin-sakubectl create clusterrolebinding dashboard-admin-sa --clusterrole=cluster-admin --serviceaccount=default:dashboard-admin-sa
```

### Get access token (optional)

If you want to use the token to login the dashboard, run the following commands to get key:

```text
kubectl get secrets# select the right dashboard-admin-sa-token to describe the secretkubectl describe secret dashboard-admin-sa-token-6nhkx
```

### Start dashboard service

```text
kubectl proxy
```

Now access Dashboard at:

> http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/

Dashboard screenshot:

![](submarine.apache.org/assets/images/kind-dashboard-96b734dca17dd1d6043efad54f4c4725.png)

[Edit this page](https://github.com/apache/submarine/edit/master/website/versioned_docs/version-0.8.0/gettingStarted/helm.md)

---

<a id="submarine-apache-org-docs-gettingstarted-notebook-index"></a>

# Jupyter Notebook | Apache Submarine

- [🏠](/)
- Getting Started
- Jupyter Notebook
Version: 0.8.0

On this page

# Jupyter Notebook

This guide describes how to use Jupyter notebook in Submarine to launch
and manage Jupyter notebooks.

## Working with notebooks

We recommend using Web UI to manage notebooks.

### Notebooks Web UI

Notebooks can be started from the Web UI. You can click the “Notebook” tab in the
left-hand panel to manage your notebooks.

![](submarine.apache.org/assets/images/notebook-list-0-7-0-dbbd48731a237cca69899bbf68f85570.png)

To create a new notebook server, click “New Notebook”. You should see a form for entering
details of your new notebook server.

- Notebook Name : Name of the notebook server. It should follow the rules below.
  1. Contain at most 63 characters.
  2. Contain only lowercase alphanumeric characters or '-'.
  3. Start with an alphabetic character.
  4. End with an alphanumeric character.
- Environment : It defines a set of libraries and docker image.
- CPU and Memory
- GPU (optional)
- EnvVar (optional) : Injects environment variables into the notebook.

If you want to use notebook-gpu-env, you should set up the gpu environment in your kubernetes.
You can install [NVIDIA/k8s-device-plugin](https://github.com/NVIDIA/k8s-device-plugin).
The list of prerequisites for running the NVIDIA device plugin is described below

- NVIDIA drivers ~= 384.81
- nvidia-docker version > 2.0
- docker configured with nvidia as the default runtime
- Kubernetes version >= 1.10

**If you’re not sure which environment you need, please choose the environment “notebook-env”
for the new notebook.**

![](submarine.apache.org/assets/images/notebook-form-0-7-0-682e8b09582336c37b0f1e18685824ab.png)

You should see your new notebook server. Click the name of your notebook server to connect to it.

![](submarine.apache.org/assets/images/created-notebook-0-7-0-73ecae05703e406a083d3791c20d19c0.png)

## Experiment with your notebook

The environment “notebook-env” includes Submarine Python SDK which can talk to Submarine Server to
create experiments, as the example below:

```python
from __future__ import print_functionimport submarinefrom submarine.client.models.environment_spec import EnvironmentSpecfrom submarine.client.models.experiment_spec import ExperimentSpecfrom submarine.client.models.experiment_task_spec import ExperimentTaskSpecfrom submarine.client.models.experiment_meta import ExperimentMetafrom submarine.client.models.code_spec import CodeSpec# Create Submarine Clientsubmarine_client = submarine.ExperimentClient()# Define TensorFlow experiment specenvironment = EnvironmentSpec(image='apache/submarine:tf-dist-mnist-test-1.0')experiment_meta = ExperimentMeta(name='mnist-dist',                                 namespace='default',                                 framework='Tensorflow',                                 cmd='python /var/tf_dist_mnist/dist_mnist.py --train_steps=100',                                 env_vars={'ENV1': 'ENV1'})worker_spec = ExperimentTaskSpec(resources='cpu=1,memory=1024M',                                 replicas=1)ps_spec = ExperimentTaskSpec(resources='cpu=1,memory=1024M',                                 replicas=1)code_spec = CodeSpec(sync_mode="git", git=GitCodeSpec(url="https://github.com/apache/submarine.git"))experiment_spec = ExperimentSpec(meta=experiment_meta,                                 environment=environment,                                 code=code_spec,                                 spec={'Ps' : ps_spec,'Worker': worker_spec})# Create experimentexperiment = submarine_client.create_experiment(experiment_spec=experiment_spec)
```

You can create a new notebook, paste the above code and run it. Or, you can find the notebook [`submarine_experiment_sdk.ipynb`](https://github.com/apache/submarine/blob/master/submarine-sdk/pysubmarine/example/submarine_experiment_sdk.ipynb) inside the launched notebook session. You can open it, try it out.

After experiment submitted to Submarine server, you can find the experiment jobs on the UI.

[Edit this page](https://github.com/apache/submarine/edit/master/website/versioned_docs/version-0.8.0/gettingStarted/notebook.md)

---

<a id="submarine-apache-org-docs-gettingstarted-python-sdk-index"></a>

# Submarine Python SDK | Apache Submarine

- [🏠](/)
- Getting Started
- Submarine Python SDK
Version: 0.8.0

On this page

# Submarine Python SDK

Submarine Python SDK can runs on any machine and it will talk to Submarine Server via REST API. So you can install Submarine Python SDK on your laptop, a gateway machine, your favorite IDE (like PyCharm/Jupyter, etc.).

Furthermore, Submarine supports an extensible package of CTR models based on **TensorFlow** and **PyTorch** along with lots of core components layers that can be used to easily build custom models. You can train any model with `model.train()` and `model.predict()`.

## Prepare Python Environment to run Submarine SDK

Submarine SDK requires Python3.7+.
It's better to use a new Python environment created by `Anoconda` or Python `virtualenv` to try this to avoid trouble to existing Python environment.
A sample Python virtual env can be setup like this:

```bash
wget https://files.pythonhosted.org/packages/33/bc/fa0b5347139cd9564f0d44ebd2b147ac97c36b2403943dbee8a25fd74012/virtualenv-16.0.0.tar.gztar xf virtualenv-16.0.0.tar.gz# Make sure to install using Python 3python3 virtualenv-16.0.0/virtualenv.py venv. venv/bin/activate
```

## Install Submarine SDK

### Install SDK from pypi.org (recommended)

Starting from `0.4.0`, Submarine provides Python SDK. Please change it to a proper version needed.

More detail: [https://pypi.org/project/apache-submarine/](https://pypi.org/project/apache-submarine/)

```bash
# Install latest stable versionpip install apache-submarine# Install specific versionpip install apache-submarine==<REPLACE_VERSION>
```

### Install SDK from source code

Please first clone code from github or go to `http://submarine.apache.org/download.html` to download released source code.

```bash
git clone https://github.com/apache/submarine.git# (optional) chackout specific branch or releasegit checkout <correct release tag/branch>cd submarine/submarine-sdk/pysubmarinepip install .
```

## Manage Submarine Experiment

Assuming you've installed submarine on K8s and forward the traefik service to localhost, now you can open a Python shell, Jupyter notebook or any tools with Submarine SDK installed.

Follow [SDK experiment example](https://github.com/apache/submarine/blob/master/submarine-sdk/pysubmarine/example/submarine_experiment_sdk.ipynb) to run an experiment.

## Training a DeepFM model

The Submarine also supports users to train an easy-to-use CTR model with a few lines of code and a configuration file, so they don’t need to reimplement the model by themself. In addition, they can train the model on both local on distributed systems, such as Hadoop or Kubernetes.

Follow [SDK DeepFM example](https://github.com/apache/submarine/blob/master/submarine-sdk/pysubmarine/example/deepfm_example.ipynb) to try the model.

[Edit this page](https://github.com/apache/submarine/edit/master/website/versioned_docs/version-0.8.0/gettingStarted/python-sdk.md)