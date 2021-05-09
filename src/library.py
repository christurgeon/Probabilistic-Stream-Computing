###########################################################
#
#   library.py
#
#   collection of probabilistic functions for node computation
#   
###########################################################


import random
import json
import os 
from time import sleep
import matplotlib.pyplot as plt
import numpy as np 


# expects to be called one level up from /src directory
def load_json(path):
    config = os.path.join(os.getcwd(), path)
    with open(config) as fin:
        data = json.load(fin)
    try:
        conn_info = set()
        if data.get("master") is None:
            raise Exception("<master> field is not preent in JSON")
        else:
            ip = data["master"]["ipaddr"]
            port = data["master"]["port"]
            conn_info.add((ip, port))
        if data.get("eos_token") is None:
            raise Exception("<eos_token> field is not present in JSON")
        if data.get("nodes") is None:
            raise Exception("<nodes> field is not present in JSON")
        ids = set()
        for node in data["nodes"]:
            if node.get("id") is None or node.get("targets") is None or node.get("ipaddr") is None or node.get("port") is None:
                raise Exception("Field missing from JSON nodes dictionary")
            id = node["id"]
            conn_tuple = (node["ipaddr"], node["port"])
            if id in ids:
                raise Exception("Duplicate <id> found")
            if conn_tuple in conn_info:
                raise Exception("Duplicate <ipaddr, port> pair found")
            ids.add(id)
            conn_info.add(conn_tuple)
    except Exception as e:
        print("An exception occurred: {}".format(e))
        raise e
    return data


def abayes(data, prior_sampler, simulate):
    for p in prior_sampler:
        if simulate(p) == data:
            yield p


def calc_normal(xs):
    xs = np.array(xs)
    mu = np.mean(xs)
    sigma = np.std(xs)
    return (mu, sigma)


def combine(dstrbs):
    mu_list = []
    sg_list = []
    for m, s in dstrbs:
        mu_list.append(m)
        sg_list.append(s)
    mu_list = np.array(mu_list)
    sg_list = np.array(sg_list)
    var_list = sg_list*sg_list
    new_mu = (np.sum(mu_list*var_list))/(np.sum(var_list))
    new_sg = np.sqrt(np.mean(var_list))
    return (new_mu, new_sg)


def uniform_prior_sampler():
    while True:
        yield random.random()


def normal_prior_sampler(mu, sigma):
    while True:
        yield np.random.normal(mu, sigma)


def finite_uniform_prior_sampler(x):
    while x > 0:
        x -= 1
        yield random.random()


def finite_normal_prior_sampler(mu, sigma, x):
    while x > 0:
        x -= 1
        yield np.random.normal(mu, sigma)


def simulate_conversions(p, N):
    """
        returns number of visitors who convert given fraction p

        it simulates N people visiting our website and tells os
        how many of them converted assuming a conversion fraction
    """
    conversions = 0
    for i in range(N):
        if random.random() < p:
            conversions += 1
    return conversions


def simulate(args):
    posterior_sample, runs = args
    return np.array([next(posterior_sample) for _ in range(runs)])


def setup_and_simulate(args):
    mu, sg, N, n_converted, runs, num_nodes = args
    runs_to_send = runs/num_nodes
    # generate posterior
    while runs_to_send > 0:
        runs_to_send -= 1
        yield (mu, sg, N, n_converted)

def stream_abayes(args):
    mu, sg, N, data = args
    p = np.random.normal(mu, sg)
    sim = simulate_conversions(p, N)
    while sim != data:
        p = np.random.normal(mu, sg)
        sim = simulate_conversions(p, N)
    yield p


def combine(args):
    run_list = args
    all_run_vals = np.ravel(run_list)
    plt.figure()
    plt.hist(all_run_vals, bins=100)
    plt.gca().set(title='Frequency Histogram with 3 nodes (1 compute)', ylabel='Frequency')
    plt.savefig("m01s03N1000.png")
    plt.show()
    return None


def combine_2d(args):
    print(args)
    run_list = args
    all_run_vals = np.array(run_list)
    print(all_run_vals)
    print(len(all_run_vals))
    return None

def conditional_sampler(s_ind, c_ind, current_point, means, covar):
    a = covar[s_ind, s_ind]
    b = covar[s_ind, c_ind]
    c = covar[c_ind, c_ind]
    mu = np.mean[s_ind] + (b * (current_point[c_ind] - means[c_ind]))/c
    sigma = np.sqrt(a-(b**2)/c)
    new_point = np.copy(current_point)
    new_point[s_ind] = np.random.randn()*sigma + mu
    return new_point


def observe_gaussian_2d(args):
    observation, means, covar, sampler, runs, num_nodes = args
    runs_to_send = runs/num_nodes
    next_point = np.array(observation)
    while runs_to_send > 0:
        next_point = sampler(0, 1, next_point, means, covar)
        next_point = sampler(1, 0, next_point, means, covar)
        yield next_point


def stream_observe_gaussian_2d(args):
    observation, means, covar, sampler = args
    # runs_to_send = runs/num_nodes
    next_point = np.array(observation)
    next_point = sampler(0, 1, next_point, means, covar)
    next_point = sampler(1, 0, next_point, means, covar)
    yield next_point

def setup_and_observe(args):
    observation, means, covar, sampler, runs, num_nodes = args
    runs_to_send = runs/num_nodes
    while runs_to_send > 0:
        runs_to_send -= 1
        yield (observation, means, covar, sampler)


# Sample experiment main
if __name__ == "__main__":
    n_converted = 20
    N = 400
    print("sc", simulate_conversions(0.1))

    posterior_sample = abayes(n_converted, uniform_prior_sampler(), simulate_conversions)
    xs = [next(posterior_sample) for _ in range(N)]

    plt.hist(xs, bins=100)
    plt.gca().set(title='Frequency Histogram', ylabel='Frequency')
    plt.savefig("fig1.png")

    plt.figure()
    posterior_sample = abayes(n_converted, normal_prior_sampler(0.0, 0.1), simulate_conversions)
    xs = [next(posterior_sample) for _ in range(N)]

    plt.hist(xs, bins=100)
    plt.gca().set(title='Frequency Histogram', ylabel='Frequency')
    plt.savefig("fig2.png")
