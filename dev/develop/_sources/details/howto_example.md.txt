# Example configuration of mount access to the NSC Sigma HPC system

## 1 Set up *httk* on your own control machine (e.g., your laptop)

* Follow the instructions on https://httk.org to get *httk₂* installed.

* If you have not done so already, make sure to initialize an identity for yourself:

    ```bash
    httk init --name 'Your Name' --email 'you@example.org'
    ```

* Print out the public key for your identity:

    ```bash
    httk identity key
    ```
    Where in the last step you see your Ed25519 public key needed below.
    Your private identity key stays on the client machine.

## 2 Set up *httk* on the HPC cluster (Sigma)

A complication for this HPC system is that its system Python is quite old, so we need a newer Python installed via miniforge.

* Do the setup as follows:

    ```bash
    mkdir -p "$HOME/sandbox/setup"
    module load Miniforge/24.7.1-2-hpc1
    mamba create -y -p "$HOME/sandbox/setup/condaenv" python=3.12
    mamba activate "$HOME/sandbox/setup/condaenv"
    python -m venv "$HOME/sandbox/setup/venv"
    source "$HOME/sandbox/setup/venv/bin/activate"
    python -m pip install --upgrade pip
    python -m pip install --upgrade \
      'httk-core @ git+https://github.com/httk/httk-core@develop' \
      'httk-atomistic @ git+https://github.com/httk/httk-atomistic@develop' \
      'httk-workflow @ git+https://github.com/httk/httk-workflow@develop' \
      'httk-workflow-vasp @ git+https://github.com/httk/httk-workflow-vasp@main'
    ```
    And then an activation script to activate this environment:
    ```bash
    cat > "$HOME/sandbox/setup/setup.sh" <<'EOF'
    module load Miniforge/24.7.1-2-hpc1
    mamba activate "$HOME/sandbox/setup/condaenv"
    source "$HOME/sandbox/setup/venv/bin/activate"
    EOF
    ```

* Also initialize your identity also on the HPC system:

    ```
    httk init --name 'Your Name' --email 'you@example.org'
    ```

## 3 On the HPC system: create the workspace and install the VASP workflows

We name the workspace we will use `main`, and install two VASP workflows, `vasp-general` and `vasp-relax`, available from the `workflows-vasp` GitHub repository:

```bash
httk workspace init --name main "$HOME/sandbox/main"
httk workflow install --workspace main 'git+https://github.com/httk/workflows-vasp@2ebfb1e3ea1d094e1c9c11639509f5a48922c932#vasp-general'
httk workflow install --workspace main 'git+https://github.com/httk/workflows-vasp@8d1063017f944ffd8b6b7aa411d9a40efbcc6d4f#vasp-relax'
```
We configure the workspace with necessary system settings for these workflows:
```bash
httk workspace settings set --key vasp.pseudo_library --value /software/sse2/tetralith_el9/manual/vasp/POTCARs/PBE/2024-03-19 main
httk workspace settings set --key vasp.command --value 'vasp_std' main
```
Jobs created on the client must reference workflows installed on sigma, including the same pinned revision. `vasp.general` runs the input files supplied by the job; `vasp.relax` supplies the relaxation sequence.

**Note:** Do not put `srun` or `mpirun` in `vasp.command`; this is configured in another setting for MPI launching.

## 4 On the HPC system: create the small and large allocation profiles

Create the two global Slurm launchers. These configure settings are adapted specifically to Sigma.
Other HPC systems will require these to be adjusted.

```bash
SLURM_ACCOUT="<your slurm account>"

httk launcher add --template slurm --global small --set slurm.nodes=2 --set slurm.ntasks=64 --set slurm.ntasks_per_node=32
httk launcher configure small \
    --set "slurm.account=$SLURM_ACCOUNT" \
    --set slurm.cpus_per_task=1 \
    --set slurm.time_limit=0:30:00 \
    --set manager.workers=1 \
    --set manager.confine=bwrap \
    --set manager.launch_mpi=pmi2 \
    --set confine.isolate_network=false \
    --set "confine.readonly_paths=/etc:$HOME/sandbox/setup:/usr:/software:/sys" \
    --set confine.devices=/dev/hfi1_0 \
    --set 'confine.environment.FI_PROVIDER=psm2,shm' \
    --set 'confine.environment.FI_PROVIDER_PATH=/software/sse2/tetralith_el9/easybuild/pure/software/impi/2021.9.0-intel-compilers-2023.1.0/mpi/2021.9.0/libfabric/lib/prov' \
    --set 'environment.prelude=source ~/sandbox/setup/setup.sh && module load VASP/6.6.0.06032026-omp-hpc2-intel-2023a-eb'

httk launcher add --template slurm --global large --set slurm.nodes=6 --set slurm.ntasks=192 --set slurm.ntasks_per_node=32
httk launcher configure large \
    --set "slurm.account=$SLURM_ACCOUNT" \
    --set slurm.cpus_per_task=1 \
    --set slurm.time_limit=23:59:59 \
    --set manager.workers=1 \
    --set manager.confine=bwrap \
    --set manager.launch_mpi=pmi2 \
    --set confine.isolate_network=false \
    --set "confine.readonly_paths=/etc:$HOME/sandbox/setup:/usr:/software:/sys" \
    --set confine.devices=/dev/hfi1_0 \
    --set 'confine.environment.FI_PROVIDER=psm2,shm' \
    --set 'confine.environment.FI_PROVIDER_PATH=/software/sse2/tetralith_el9/easybuild/pure/software/impi/2021.9.0-intel-compilers-2023.1.0/mpi/2021.9.0/libfabric/lib/prov' \
    --set 'environment.prelude=source ~/sandbox/setup/setup.sh && module load VASP/6.6.0.06032026-omp-hpc2-intel-2023a-eb'
```
(For launchers that already exist, use `httk launcher configure NAME --set ...` to alter their configuration.)

```bash
httk workspace settings set --key manager.launch --value small main
```
You can take a look at the configurations we set up by:
```bash
httk launcher show small large --json
```
The important settings are:

| Setting | Purpose |
| --- | --- |
| `environment.prelude` | Activate httk and load the tested VASP module before the manager starts. |
| `manager.launch_mpi=pmi2` | Use Slurm's PMI2 launch interface for Intel MPI. |
| `confine.environment.FI_PROVIDER_PATH` | Make Intel MPI's libfabric provider directory discoverable. |
| `confine.environment.FI_PROVIDER=psm2,shm` | Retain the tested native Omni-Path provider selection. |
| `/software` read-only mount | Expose VASP, MPI, runtime libraries and POTCARs. |
| `/sys` read-only mount | Expose the hardware information used by the fabric runtime. |
| `/dev/hfi1_0` device mount | Allow access to the Omni-Path HFI device. |
| `confine.isolate_network=false` | Keep the network available for communication between nodes. |

The prelude belongs on each approved launcher.

## 5 Configure and start the workspace daemon

Configure the workspace daemon (only needed if you are actually going to transfer jobs and start them via sshfs or similar!).
The following configuration is carefully created for Sigma and will need to be adapted to other HPC systems.
```bash
CLIENT_PUBLIC_KEY='ed25519:<THE PUBLIC KEY FOR YOUR IDENTITY FROM STEP 1>'

httk workspace daemon init main \
  --add launchers=small \
  --add launchers=large \
  --set "authorized_keys=$CLIENT_PUBLIC_KEY" \
  --set bwrap=/usr/bin/bwrap \
  --set "python=$HOME/sandbox/setup/venv/bin/python3" \
  --set sbatch=/usr/bin/sbatch \
  --set squeue=/software/tools/bin/squeue \
  --set scancel=/usr/bin/scancel \
  --set sacct=/usr/bin/sacct \
  --set slurm_conf=/etc/slurm/slurm.conf \
  --set max_submissions=128

httk workspace daemon check main
httk workspace daemon run main
```
Where the last command starts the deamon. As you are starting things up/debugging, it may be nice to run it this way in the foreground.
However, as you get more used to the setup, it is recommended to start it in the background in a screen process like this:
```bash
screen -dm httk workspace daemon run main
```
The deamon process must remain available to answer requests and submit managers. The Slurm managers execute and return jobs independently of the daemon.

To modify a deamon configuration, do e.g.,:
```bash
httk workspace daemon configure main --add launchers=large
httk workspace daemon configure main --add "authorized_keys=<ANOTHER KEY>"
httk workspace daemon reload main
```
`reload` activates the changed configuration (also after `httk launcher configure`); a running `httk workspace daemon run` restarts on it by itself, and queued and running managers keep running. As a saftey feature, a deamon has a default quota of running 128 "managers".

## 6 Mount the exchange on the client

Use SSHFS (or any other filesystem mount mechanism) to mount the exchange directory:
```bash
SIGMA_USER='YOUR_SIGMA_USERNAME'
SIGMA_HOST='YOUR_SIGMA_SSH_HOST_OR_ALIAS'
EXCHANGE_MOUNT="$HOME/httk-mounts/sigma-exchange"

mkdir -p "<EXCHANGE_MOUNT_DIR>"
sshfs <SIGMA_USERNAME>@sigma.nsc.liu.se:/home/<SIGMA_USERNAME>/sandbox/main/exchange" "<EXCHANGE_MOUNT_DIR>"
ls "$<EXCHANGE_MOUNT_DIR>/exchange.json" "$<EXCHANGE_MOUNT_DIR>/daemon.json"
```
Be careful to *only* mount the exchange directory as shown above, not the whole workspace! (That would negate some of the security design around this way of transferring jobs).

## 7 Configure the client remote

**Client.** With the exchange mounted and the sigma daemon running:

```bash
httk remote add --template mount-daemon sigma
httk remote daemon configure sigma --exchange "<EXCHANGE_MOUNT_DIR>"
httk remote check sigma
httk remote daemon health sigma
```

## 8 Send jobs and choose an allocation

Create a new job in your local workspace using one of the exact workflow references installed in step 3. Install the workflow into the local workspace when creating it. Then eject the job, replacing `JOB` with its job ID or key:
```bash
httk job eject JOB "$EXCHANGE_MOUNT/inbox"
```
Launch a manager running in a small allocation:
```bash
httk remote daemon start sigma --configuration small
```
Or launch a manager running in a large allocation:
```bash
httk remote daemon start sigma --configuration large
```
Job and step resource requirements determine how many ranks a calculation receives inside the allocation. Starting `large` does not automatically give every job 192 ranks.

Monitor the managers:
```bash
httk remote daemon status sigma
httk remote daemon log sigma --handle MANAGER_HANDLE
```

A manager adopts the inbox bundle and returns the finished job tree to `outbox`. From the local workspace where you want the result, adopt it using the actual path visible in the exchange:
```bash
httk job adopt --move "$EXCHANGE_MOUNT/outbox/CLIENT_JOB_KEY"
```
`CLIENT_JOB_KEY` is your own job key, `<tag>--<client job UUID>`, the same name the bundle had in `inbox/`.
The `--move` removes the exchange copy after successful adoption. If a start request times out, retry the same request ID with the same fields. Use a new ID for a genuinely new start.

To change an allocation's time limit, for example:

```bash
# Sigma
httk launcher configure large --set slurm.time_limit=02:00:00
```

A single start can instead ask for a shorter allocation, or a longer one up to the launcher's `slurm.time_limit_max`:

```bash
httk remote daemon start sigma --configuration large --time-limit 00:30:00
```

## Reference

The environment-specific settings above come from the working sigma configuration established on 11 October 2026. Workflow references remain pinned to the supplied revisions. The package installation commands use maintained development branches rather than a full dependency freeze.

- [Workspace daemon setup and client protocol](workspace_daemon.md)
- [Mounted exchange remote](remotes.md#mounted-exchange-with-a-confined-daemon-mount-daemon)
- [Launcher settings](launchers.md)

