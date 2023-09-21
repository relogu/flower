import json
import pickle
from collections import OrderedDict

import torch

import flwr as fl
from centralized import get_model, train, validate
from data import load_datasets
from config import PARAMS

DEVICE = torch.device(PARAMS.device)  # Try "cuda" to train on GPU
print(
    f"Training on {DEVICE} using PyTorch {torch.__version__} and Flower {fl.__version__}"
)
TRAINLOADERS = load_datasets(PARAMS.iid)


def get_parameters(net):
    return [val.cpu().numpy() for _, val in net.state_dict().items()]


def set_parameters(net, parameters):
    params_dict = zip(net.state_dict().keys(), parameters)
    state_dict = OrderedDict({k: torch.Tensor(v) for k, v in params_dict})
    net.load_state_dict(state_dict, strict=True)


def save_personalization_weight(cid, model, personalization_layers):
    weights = get_parameters(model)
    # save weight
    personalized_weight = weights[len(weights) - personalization_layers :]
    with open(f"Per_{cid}.pickle", "wb") as file_weight:
        pickle.dump(personalized_weight, file_weight)
    file_weight.close()


def load_personalization_weight(cid, model, personalization_layers):
    import pickle

    weights = get_parameters(model)
    with open(f"Per_{cid}.pickle", "rb") as file_weight:
        personalized_weight = pickle.load(file_weight)
        file_weight.close()
    weights[len(weights) - personalization_layers :] = personalized_weight

    # set new weight to the model
    set_parameters(model, weights)


class FlowerClient(fl.client.NumPyClient):
    def __init__(
        self, model, trainloader, cid, timesteps, epochs, device, personalization_layers
    ):
        self.model = model
        self.trainloader = trainloader
        self.cid = cid
        self.timesteps = timesteps
        self.epochs = epochs
        self.device = device
        self.personalization_layers = personalization_layers

    def get_parameters(self, config):
        return get_parameters(self.model)

    def fit(self, parameters, config):
        # Update local model parameters
        set_parameters(self.model, parameters)

        # Read values from config
        server_round = config["server_round"]

        print("Personalized_layers: ", self.personalization_layers)

        cpu = False
        if PARAMS.device == "cpu":
            cpu = True

        # Update local model parameters
        if int(server_round) > 1 and PARAMS.personalized:
            load_personalization_weight(
                self.cid, self.model, self.personalization_layers
            )

        train(
            self.model,
            self.trainloader,
            self.cid,
            server_round,
            self.epochs,
            self.timesteps,
            cpu,
        )
        if PARAMS.personalized:
            save_personalization_weight(self.cid, self.model, self.personalization_layers)

        return get_parameters(self.model), len(self.trainloader), {}

    def evaluate(self, parameters, config):
        # Update local model parameters
        set_parameters(self.model, parameters)

        server_round = config["server_round"]

        precision, recall = validate(
            self.model, self.cid, server_round, self.timesteps, self.device
        )
        results = {
            "precision": precision,
            "recall": recall,
            "cid": self.cid,
            "server_round": server_round,
        }
        json.dump(results, open("logs.json", "a"))

        loss = 1.0
        num_examples_test = 100
        return (
            loss,
            num_examples_test,
            {"precision": precision, "recall": recall, "cid": self.cid},
        )


def client_fn(cid):
    """Create a Flower client representing a single organization."""

    timesteps = PARAMS.num_inference_steps  # diffusion model decay steps
    epochs = PARAMS.num_epochs  # training epochs

    # Load model
    model = get_model().to(DEVICE)
    personalization_layers = 4

    # Load data (CIFAR-10)
    # Note: each client gets a different trainloader/valloader, so each client
    # will train and evaluate on their own unique data
    trainloader = TRAINLOADERS[int(cid)]

    # Create a  single Flower client representing a single organization
    return FlowerClient(
        model,
        trainloader,
        cid,
        timesteps,
        epochs,
        PARAMS.device,
        personalization_layers,
    )
