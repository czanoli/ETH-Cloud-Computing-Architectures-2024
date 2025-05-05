# Cloud Computing Architecture Project 2024

This repository contains starter code for the Cloud Computing Architecture course project at ETH Zurich. Students will explore how to schedule latency-sensitive and batch applications in a cloud cluster. Please follow the instructions in the project handout. 

# Instructions

1. Log-in to your Google Cloud account with `gcloud init`.
    When prompted to "configure a default Compute Region and Zone", say *NO*.
2. Do the ADC thingy:
   `gcloud auth application-default login`
2. Create a bucket for the kops state:
   `source scripts/env && gsutil mb $KOPS_STATE_STORE`
3. Setup the kubernetes cluster with `scripts/bootstrap`
4. Setup the memcached clients with `scripts/memcached`
5. Run the load with `scripts/load-memached` and in another terminal, gather
   the measurements with `scripts/measure-memcached`.
6. Bring down the cluster with `scripts/delete`.


# Part 3 Instructions

1. `scripts/bootstrap`
2. `scripts/memcached`
3. `scripts/load-memcached` (in one terminal)
4. `scripts/measure` (in another terminal)
5. `scripts/schedule`       (in another terminal)

# Part 4 Instructions

1. `scripts/bootstrap` start the machines
2. `scripts/memcached` setup memcached

(for part 1)
3. `scripts/measure-1`

(for part 1d)
3. `scripts/measure-1d`

(for part 3+)
3. `scripts/scheduler`
