from __future__ import print_function
from collections import defaultdict
import os
import sys


def get_subjobs(job):
    """Return all subjobs of a job.

    Returns a list of a single job if job has no subjobs.
    """
    subjobs = job.subjobs
    # So that we can use the same logic for jobs with and without subjobs
    if not subjobs:
        subjobs = [job]
    return subjobs


def download_lfns(job, path="/afs/cern.ch/work/a/apearce/Ganga", matcher=None):
    """Downloads all Dirac files for all subjobs of a given job.

    Keyword arguments:
    job -- Ganga Job object. Only completed jobs/subjobs will be downloaded
    path -- Prefix path to download to, will be suffixed by job_id/subjob_id
    matcher -- Method that is given each DiracFile object that has an LFN,
               returning True if the file should be downloaded
    """
    if matcher is None:
        matcher = lambda df: True

    print("Downloading LFNs to {0}/{1}".format(path, job.id))
    # Collect all output files
    if job.subjobs:
        outputs = [
            (sj.id, sj.outputfiles)
            for sj in job.subjobs
            if sj.status == "completed"
        ]
        print("Will download {0}/{1} subjobs".format(
            len(outputs), len(job.subjobs)
        ))
    else:
        outputs = [(0, job.outputfiles)] if job.status == "completed" else []
    # Download all the DiracFile objects in each output file list
    for jid, output in outputs:
        for df in output.get(DiracFile):
            if not df.lfn:
                print("Skipping job {0} subjob {1}".format(job.id, jid))
                print("No output data LFN")
                continue
            if not matcher(df):
                print("Skipping job {0} subjob {1} file {2}".format(
                    job.id, jid, df.lfn
                ))
                print("Not matched by matcher method")
                continue
            local = os.path.join(path, str(job.id), str(jid))
            # Don't download to file if the dir exists
            # Helpful if a job download is interrupted; just delete
            # the failed subjobs and call this method again
            try:
                os.makedirs(local)
            except OSError:
                print("Skipping job {0} subjob {1}".format(
                    job.id,
                    jid
                ))
                print("Directory exists: {0}".format(local))
                continue
            # Behaviour seems to have changed in Ganga 6.1; rather than
            # specifying the localDiro n the DiracFile object, the path should
            # be passed to the get method
            # This is the only way I can find of getting the version number
            if '6-0' in Ganga.Runtime.bootstrap._gangaVersion:
                df.localDir = local
                df.get()
            else:
                df.get(localPath=local)


def delete_lfns(job):
    """Deletes all grid files belonging to the job."""
    print('Deleting LFNs of job %i' % (job.id))
    # Collect all output files
    if job.subjobs:
        outputs = [
            sj.outputfiles
            for sj in job.subjobs
            if sj.status == "completed"
        ]
    else:
        outputs = [job.outputfiles] if job.status == "completed" else []
    for output in outputs:
        for df in output.get(DiracFile):
            # Check the object has an associated LFN
            if df.lfn:
                df.remove()


def print_incomplete(job):
    """Prints all subjobs with a status other than 'completed'."""
    for sj in job.subjobs:
        if sj.status != 'completed':
            print(sj.id, sj.status)


def resubmit_failed(job):
    """Resubmits all subjobs with status 'failed'."""
    for sj in job.subjobs:
        if sj.status == 'failed':
            print('Resubmitting subjob %i' % (sj.id))
            sj.resubmit()


def reset_failed(job):
    """Calls backend.reset() on all subjobs with status 'failed'."""
    for sj in job.subjobs:
        if sj.status == 'failed':
            print('Resetting subjob %i' % (sj.id))
            sj.backend.reset()


def replicate_to_cern(job):
    """Replicate all DiracFiles to the CERN-USER storage element."""
    for sj in get_subjobs(job).select(status='completed'):
        for df in sj.outputfiles.get(DiracFile):
            # If the grid file isn't already at CERN, replicate it there
            if 'CERN-USER' not in df.locations:
                df.replicate('CERN-USER')


def write_lfns(job, fname, cern_only=True):
    """Write the LFNs of all DiracFiles on CERN-USER to a file."""
    lfns = []
    for sj in get_subjobs(job).select(status='completed'):
        for df in sj.outputfiles.get(DiracFile):
            if 'CERN-USER' in df.locations or not cern_only:
                lfns.append(df.lfn)

    with open(fname, 'w') as f:
        f.writelines(lfn + '\n' for lfn in lfns)


def write_access_urls(job, fname):
    """Write the XRootD access URLs of all DiracFiles to a file."""
    pfns = []
    for sj in get_subjobs(job).select(status='completed'):
        for df in sj.outputfiles.get(DiracFile):
            pfns += df.accessURL()

    with open(fname, 'w') as f:
        f.writelines(pfn + '\n' for pfn in pfns)


def print_status(job):
    """Print the status count of all subjobs."""
    status_dict = defaultdict(int)
    for sj in get_subjobs(job):
        status_dict[sj.status] += 1
    for k, v in status_dict.iteritems():
        print('{0}: {1}'.format(k, v))


def diracfile_root_matcher(df):
    """Return True if DiracFile.namePattern ends with `.root`."""
    return df.namePattern.endswith('.root')


def access_urls(job, matcher=diracfile_root_matcher):
    """Return a list of all DiracFile URLs that pass the matcher.

    matcher should be a method that accepts a DiracFile and returns a boolean.
    """
    urls = []
    for sj in get_subjobs(job).select(status='completed'):
        outputs = sj.outputfiles.get(DiracFile)
        for df in filter(matcher, outputs):
            try:
                urls.append(df.accessURL())
            except KeyError:
                print('Could not get accessURL for {0}'.format(sj.id))

    return urls
