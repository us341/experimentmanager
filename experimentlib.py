"""
<Program Name>
  experimentlib.py

<Authors>
  Justin Samuel

<Date Started>
  December 1, 2009 

<Purpose>
  A library for conducting experiments using Seattle vessels. The functions in
  this library allow for communicating with vessels (e.g. to upload files and
  run programs) as well as for communicating with SeattleGENI (e.g. to obtain
  vessels to run experiments on).
  
<Usage>
  Ensure that this file is in a directory containing the seattlelib files as
  well as the seattlegeni_xmlrpc.py module. In your own script, add:
  
    import experimentlib
    
  then call the methods desired.
  
  Note that if your script resides outside of the directory that contains the
  seattlelib files,  experimentlib.py, and seattlegeni_client.py, then you'll
  need to set that directory/those directories in your python path. For example,
  if you downloaded an installer (even if you haven't installed Seattle on the
  machine this script resides on, the path will be to the seattle_repy directory
  that was among the extracted installer files. To set the path directly in your
  script rather than through environment variables, you can use something like:

    import sys
    sys.path.append("/path/to/seattle_repy") 
    
  You would need to do the above *before* your line that says:

    import experimentlib

  For examples of using this experimentlib, see the examples/ directory.

<Notes>

  Object Definitions:
  
    * vesselhandle: a vesselhandle is a string that contains the information
      required to uniquely identify a vessel, regardless of the current
      location (IP address) of the node the vessel is on. This is in the format
      of "nodeid:vesselname".

    * nodeid: a string that contains the information required to uniquely
      identify a node, regardless of its current location.
      
    * vesselname: a string containing the name of a vessel. This name will
      be unique on any given node, but the same likely is used for vessels
      on other nodes. Thus, this does not uniquely identify a vessel, in
      general. To uniquely identify a vessel, a vesselhandle is needed.

    * nodelocation: a string containing the location of a node. This will not
      always be "ip:port". It could, for example, be "NATid:port" in the case
      of a node that is accessible through a NAT forwarder.
      
    * vesseldict: a dictionary of details related to a given vessel. The keys
      that will always be present are 'vesselhandle', 'nodelocation',
      'vesselname', and 'nodeid'. Additional keys will be present depending on
      the function that returns the vesseldict. See the individual function
      docstring for details.
      
    * identity: a dictionary that minimally contains a public key but may also
      contain the related private key and the username of a corresponding
      SeattleGENI account. When one wants to perform any operation that would
      require a public key, private key, or username, an identity must be
      provided. An identity can be created using the functions named
      create_identity_from_*.
      
    * monitorhandle: an object that can be provided to various functions to
      update or modify a previously created vessel status monitor.
  
  Exceptions:
  
    All exceptions raised by functions in this module will either be or extend:
      * SeattleExperimentError
      * SeattleGENIError
      
    The SeattleGENIError* exceptions will only be raised by the functions whose
    names are seattlegeni_*. Any of the seattlegeni_* functions may raise the
    following in addition to specific exceptions described in the function
    docstrings (these are all subclasses of SeattleGENIError):
      * SeattleGENICommunicationError
      * SeattleGENIAuthenticationError
      * SeattleGENIInvalidRequestError
      * SeattleGENIInternalError
      
    In the case of invalid arguments to functions, the following may be
    raised (these will not always be documented for each function):
      * TypeError
      * ValueError
      * IOError (if the function involves reading/writing files and the
                 filename provided is missing/unreadable/unwritable)
      
    For the specific exceptions raised by a given function, see the function's
    docstring.
"""

import os
import random
import sys
import threading
import traceback
import xmlrpclib

import seattlegeni_xmlrpc

from repyportability import *
import repyhelper

repyhelper.translate_and_import("nmclient.repy")
repyhelper.translate_and_import("time.repy")
repyhelper.translate_and_import("rsa.repy")
repyhelper.translate_and_import("listops.repy")
repyhelper.translate_and_import("parallelize.repy")
repyhelper.translate_and_import("domainnameinfo.repy")
repyhelper.translate_and_import("advertise.repy")   #  used to do OpenDHT lookups

# The maximum number of node locations to return from a call to lookup_node_locations.
max_lookup_results = 1024 * 1024

# The timeout to use for communication, both in advertisement lookups and for
# contacting nodes directly.
defaulttimeout = 10

# The number of worker threads to use for each parallelized operation.
num_worker_threads = 5

# Whether additional information and debugging messages should be printed
# to stderr by this library.
print_debug_messages = True

# OpenDHT can be slow/hang, which isn't fun if the experimentlib is being used
# interactively. So, let's default to central advertise server lookups here
# until we're sure all issues with OpenDHT are resolved.
# A value of None indicates the default of ['opendht', 'central'].
#advertise_lookup_types = None
advertise_lookup_types = ['central']

# A few options to be passed along to the SeattleGENI xmlrpc client.
# None means the default.
SEATTLEGENI_XMLRPC_URL = None
SEATTLEGENI_ALLOW_SSL_INSECURE = None # Set to True to allow insecure SSL.
SEATTLEGENI_CA_CERTS_FILES = None

# These constants can be used as the type argument to seattlegeni_acquire_vessels.
SEATTLEGENI_VESSEL_TYPE_WAN = "wan"
SEATTLEGENI_VESSEL_TYPE_LAN = "lan"
SEATTLEGENI_VESSEL_TYPE_NAT = "nat"
SEATTLEGENI_VESSEL_TYPE_RAND = "rand"

# TODO: Are there any other status actually used? nmstatusmonitory.py shows
# that ThreadErr and Stale may be possibilities. How about others?
VESSEL_STATUS_FRESH = "Fresh"
VESSEL_STATUS_STARTED = "Started"
VESSEL_STATUS_STOPPED = "Stopped"
VESSEL_STATUS_TERMINATED = "Terminated"
VESSEL_STATUS_NO_SUCH_NODE = "NO_SUCH_NODE"
VESSEL_STATUS_NO_SUCH_VESSEL = "NO_SUCH_VESSEL"
VESSEL_STATUS_NODE_UNREACHABLE = "NODE_UNREACHABLE"

# For convenience we define two sets of vessel status constants that include
# all possible statuses grouped by whether the status indicates the vessel is
# usable/active or whether it is unusable/inactive.
VESSEL_STATUS_SET_ACTIVE = set([VESSEL_STATUS_FRESH, VESSEL_STATUS_STARTED,
                                VESSEL_STATUS_STOPPED, VESSEL_STATUS_TERMINATED])
VESSEL_STATUS_SET_INACTIVE = set([VESSEL_STATUS_NO_SUCH_NODE, VESSEL_STATUS_NO_SUCH_VESSEL,
                                  VESSEL_STATUS_NODE_UNREACHABLE])

# Whether _initialize_time() has been called.
_initialize_time_called = False

# Keys are node locations (a string of "host:port"), values are nmhandles.
# Note that this method of caching nmhandles will cause problems if multiple
# identities/keys are being used to contact the name node.
_nmhandle_cache = {}

# Keys are monitor ids we generate. Values are dicts with the following keys:
#    'vesselhandle_list': the list of vesselhandles this monitor is registered for.
#    'vessels': stored data related to individual vessels.
#    'waittime': the number of seconds between initiating processing of vessels.
#    'callback': a the registered callback function
#    'timerhandle': a timer handle for the registered callback
#    'concurrency': the number of threads to use to process vessels.
_vessel_monitors = {}

_monitor_lock = threading.Lock()

# TODO: Do we want a global node-communication lock to prevent
# multiple threads/monitors from talking to the same node at the
# same time? The biggest concern might be just communication errors
# from more than three simultaneous connections to the node.

# Keys are nodeids, values are nodelocations.
_node_location_cache = {}





class SeattleExperimentError(Exception):
  """Base class for other exceptions."""



class UnexpectedVesselStatusError(SeattleExperimentError):
  """
  When a vessel status is reported by a node and that status is something
  we don't understand. Mostly this is something we care about because we
  want to definitely tell users what to expect in their code in terms of
  status, so we should be very clear about the possibly values and never
  have to raise this exception.
  """



class NodeCommunicationError(SeattleExperimentError):
  """Unable to perform a requested action on/communication with a node/vessel."""



class NodeLocationLookupError(SeattleExperimentError):
  """
  Unable to determine the location of a node based on its nodeid or unable
  to successfully perform an advertisement lookup.
  """


  
class IdentityInformationMissingError(SeattleExperimentError):
  """
  The information that is part of an identity object is incomplete. For
  example, if only the public key is in the identity but the identity is
  used in a way that requires a private key, this exception would be
  raised.
  """


#This is the base class for all SeattleGENI errors. We make this available
#in the namespace of the experimentlib so that clients do not have to import
#seattlegeni_xmlrpc to catch these.
SeattleGENIError = seattlegeni_xmlrpc.SeattleGENIError

# We make these available, as well, in case users find them useful. We prefix
# all of these error names with SeattleGENI.
SeattleGENICommunicationError = seattlegeni_xmlrpc.CommunicationError
SeattleGENIInternalError = seattlegeni_xmlrpc.InternalError
SeattleGENIAuthenticationError = seattlegeni_xmlrpc.AuthenticationError
SeattleGENIInvalidRequestError = seattlegeni_xmlrpc.InvalidRequestError
SeattleGENINotEnoughCreditsError = seattlegeni_xmlrpc.NotEnoughCreditsError
SeattleGENIUnableToAcquireResourcesError = seattlegeni_xmlrpc.UnableToAcquireResourcesError





def _validate_vesselhandle(vesselhandle):
  if not isinstance(vesselhandle, basestring):
    raise TypeError("vesselhandle must be a string, not a " + str(type(vesselhandle)))
  
  parts = vesselhandle.split(':')
  if len(parts) != 2:
    raise ValueError("vesselhandle '" + vesselhandle + "' invalid, should be nodeid:vesselname")





def _validate_vesselhandle_list(vesselhandle_list):
  if not isinstance(vesselhandle_list, list):
    raise TypeError("vesselhandle list must be a list, not a " + str(type(vesselhandle_list)))

  for vesselhandle in vesselhandle_list:
    _validate_vesselhandle(vesselhandle)





def _validate_nodelocation(nodelocation):
  if not isinstance(nodelocation, basestring):
    raise TypeError("nodelocation must be a string, not a " + str(type(nodelocation)))
  
  parts = nodelocation.split(':')
  if len(parts) != 2:
    raise ValueError("nodelocation '" + nodelocation + "' invalid, should be host:port")





def _validate_nodelocation_list(nodelocation_list):
  if not isinstance(nodelocation_list, list):
    raise TypeError("nodelocation list must be a list, not a " + str(type(nodelocation_list)))

  for nodelocation in nodelocation_list:
    _validate_nodelocation(nodelocation)





def _validate_identity(identity, require_private_key=False, require_username=False):
  if not isinstance(identity, dict):
    raise TypeError("identity must be a dict, not a " + str(type(identity)))

  if 'publickey_str' not in identity:
    raise TypeError("identity dict doesn't have a 'publickey_str' key, so it's not an identity.")

  if require_private_key:
    if 'privatekey_str' not in identity:
      raise IdentityInformationMissingError("identity must have a private key for the requested operation.")

  if require_username:
    if 'username' not in identity:
      raise IdentityInformationMissingError("identity must have a username for the requested operation.")





def _debug_print(msg):
  print >> sys.stderr, msg





def _initialize_time():
  """
  Does its best to call time_updatetime() and raises a SeattleExperimentError
  if it doesn't succeed after many tries.
  """
  global _initialize_time_called
  
  if not _initialize_time_called:
    
    max_attempts = 10
    possible_ports = range(10000, 60001)
    
    # Ports to use for UDP listening when doing a time update.
    portlist = random.sample(possible_ports, max_attempts) 
    
    for localport in portlist:
      try:
        time_updatetime(localport)
        _initialize_time_called = True
        return
      except TimeError:
        error_message = traceback.format_exc()
    
    raise SeattleExperimentError("Failed to perform time_updatetime(): " + error_message)





def _create_list_from_key_in_dictlist(dictlist, key):
  """
  List comprehensions are verboten by our coding style guide (generally for
  good reason). Otherwise, we wouldn't have function and would just write the
  following wherever needed:
    [x[key] for x in dictlist]
  """
  new_list = []
  for dictitem in dictlist:
    new_list.append(dictitem[key])
  return new_list





def _get_nmhandle(nodelocation, identity=None):
  """
  Get an nmhandle for the nodelocation and identity, if provided. This will look
  use a cache of nmhandles and only create a new one if the requested nmhandle
  has not previously been requested.
  """
  
  # Call _initialize_time() here because time must be updated at least once before
  # nmhandles are used.
  _initialize_time()
  
  host, port = nodelocation.split(':')
  port = int(port)
  
  if identity is None:
    identitystring = "None"
  else:
    identitystring = identity['publickey_str']
    
  if identitystring not in _nmhandle_cache:
    _nmhandle_cache[identitystring] = {}

  if nodelocation not in _nmhandle_cache[identitystring]:
    try:
      if 'privatekey_dict' in identity:
        nmhandle = nmclient_createhandle(host, port, privatekey=identity['privatekey_dict'],
                                           publickey=identity['publickey_dict'], timeout=defaulttimeout)
      else:
        nmhandle = nmclient_createhandle(host, port, publickey=identity['publickey_dict'], timeout=defaulttimeout)
    except NMClientException, e:
      raise NodeCommunicationError(str(e))
    
    _nmhandle_cache[identitystring][nodelocation] = nmhandle
    
  return _nmhandle_cache[identitystring][nodelocation]





def run_parallelized(targetlist, func, *args):
  """
  <Purpose>
    Parallelize the calling of a given function using multiple threads.
  <Arguments>
    targetlist
      a list what will be the first argument to func each time it is called.
    func
      the function to be called once for each item in targetlist.
    *args
      (optional) every additional argument will be passed to func after an
      item from targetlist. That is, these will be the second, third, etc.
      argument to func, if provided. These are not required a.
  <Exceptions>
    SeattleExperimentError
      Raised if there is a problem performing parallel processing. This will
      not be raised just because func raises exceptions. If func raises
      exceptions when it is called, that exception information will be
      available through the run_parallelized's return value.
  <Side Effects>
    Up to num_worker_threads (a global variable) threads will be spawned to
    call func once for every item in targetlist.
  <Returns>
    A tuple of:
      (successlist, failurelist)
    where successlist is a list of tuples of the format:
      (target, return_value_from_func)
    and failurelist is a list of tuples of the format:
      (target, exception_string)
    Note that exception_string will not contain a full traceback, but rather
    only the string representation of the exception.
  """
  
  try:
    phandle = parallelize_initfunction(targetlist, func, num_worker_threads, *args)
  
    while not parallelize_isfunctionfinished(phandle):
      # TODO: Give up after a timeout? This seems risky as run_parallelized may
      # be used with functions that take a long time to complete and very large
      # lists of targets. It would be a shame to break a user's program because
      # of an assumption here. Maybe it should be an optional argument to 
      # run_parallelized.
      sleep(.1)
    
    results = parallelize_getresults(phandle)
  except ParallelizeError:
    raise SeattleExperimentError("Error occurred in run_parallelized: " + 
                                 traceback.format_exc())

  # These are lists of tuples. The first is a list of (target, retval), the
  # second is a list of (target, errormsg)
  return results['returned'], results['exception']

    
  



def create_identity_from_key_files(publickey_fn, privatekey_fn=None):
  """
  <Purpose>
    Create an identity from key files.
  <Arguments>
    publickey_fn
      The full path, including filename, to the public key this identity
      should represent. Note that the identity's username will be assumed
      to be the part of the base filename before the first period (or the
      entire base filename if there is no period). So, to indicate a username
      of "joe", the filename should be, for example, "joe.publickey".
    privatekey_fn
      (optional) The full path, including filename, to the private key that
      corresponds to publickey_fn. If this is not provided, then the identity
      will not be able to be used for operations the require a private key.
  <Exceptions>
    IOError
      if the files do not exist or are not readable.
    ValueError
      if the files do not contain valid keys.
  <Returns>
    An identity object to be used with other functions in this module.
  """
  identity = {}
  identity["username"] = os.path.basename(publickey_fn).split(".")[0]
  identity["publickey_fn"] = publickey_fn
  try:
    identity["publickey_dict"] = rsa_file_to_publickey(publickey_fn)
    identity["publickey_str"] = rsa_publickey_to_string(identity["publickey_dict"])
    
    if privatekey_fn is not None:
      identity["privatekey_fn"] = privatekey_fn
      identity["privatekey_dict"] = rsa_file_to_privatekey(privatekey_fn)
      identity["privatekey_str"] = rsa_privatekey_to_string(identity["privatekey_dict"])
  except IOError:
    raise
  except ValueError:
    raise

  return identity





def create_identity_from_key_strings(publickey_string, privatekey_string=None, username=None):
  """
  <Purpose>
    Create an identity from key strings.
  <Arguments>
    publickey_string
      The string containing the public key this identity should represent. The
      string must consists of the modulus, followed by a space, followed by
      the public exponent. This will be the same as the contents of a public
      key file.
    privatekey_string
      (optional) The full path, including filename, to the private key that
      corresponds to publickey_fn. If this is not provided, then the identity
      will not be able to be used for operations the require a private key.
    username
      (optional) A string containing the username to associate with this
      identity. This is only necessary if using this identity with the
      seattlegeni_* functions.
  <Exceptions>
    ValueError
      if the strings do not contain valid keys.
  <Returns>
    An identity object to be used with other functions in this module.
  """
  identity = {}
  identity["username"] = username
  try:
    identity["publickey_dict"] = rsa_string_to_publickey(publickey_string)
    identity["publickey_str"] = rsa_publickey_to_string(identity["publickey_dict"])
    
    if privatekey_string is not None:
      identity["privatekey_dict"] = rsa_string_to_privatekey(privatekey_string)
      identity["privatekey_str"] = rsa_privatekey_to_string(identity["privatekey_dict"])
  except IOError:
    raise
  except ValueError:
    raise

  return identity





def _lookup_node_locations(keystring, lookuptype=None):
  """Does the actual work of an advertise lookup."""
  
  keydict = rsa_string_to_publickey(keystring)
  try:
    if lookuptype is not None:
      nodelist = advertise_lookup(keydict, maxvals=max_lookup_results, timeout=defaulttimeout, lookuptype=lookuptype)
    else:
      nodelist = advertise_lookup(keydict, maxvals=max_lookup_results, timeout=defaulttimeout)
  except AdvertiseError, e:
    raise NodeLocationLookupError("Failure when trying to perform advertise lookup: " + 
                                  traceback.format_exc())

  # If there are no vessels for a user, the lookup may return ''.
  for nodename in nodelist[:]:
    if nodename == '':
      nodelist.remove(nodename)

  return nodelist





def lookup_node_locations_by_identity(identity):
  """
  <Purpose>
    Lookup the locations of nodes that are advertising their location under a
    specific identity's public key.
  <Arguments>
    identity
      The identity whose public key should be used to lookup nodelocations.
  <Exceptions>
    NodeLocationLookupError
      If a failure occurs when trying lookup advertised node locations.
  <Returns>
    A list of nodelocations.
  """
  _validate_identity(identity)
  keystring = str(identity['publickey_str'])
  return _lookup_node_locations(keystring, lookuptype=advertise_lookup_types)





def lookup_node_locations_by_nodeid(nodeid):
  """
  <Purpose>
    Lookup the locations that a specific node has advertised under. There may
    be multiple locations advertised if the node has recently changed location.
  <Arguments>
    nodeid
      The nodeid of the node whose advertised locations are to be looked up.
  <Exceptions>
    NodeLocationLookupError
      If a failure occurs when trying lookup advertised node locations.
  <Returns>
    A list of nodelocations.
  """
  return _lookup_node_locations(nodeid, lookuptype=advertise_lookup_types)





def find_vessels_on_nodes(identity, nodelocation_list):
  """
  <Purpose>
    Contact one or more nodes and determine which vessels on those nodes are
    usable by a given identity.
  <Arguments>
    identity
      The identity whose vessels we are interested in. This can be the identity
      of either the vessel owner or a vessel user.
    nodelocation_list
      A list of nodelocations that should be contacted. This can be an empty
      list (which will result in an empty list of vesselhandles returned).
  <Exceptions>
    SeattleExperimentError
      If an error occurs performing a parallelized operation.
  <Returns>
    A list of vesselhandles.
  """
  _validate_identity(identity)
  _validate_nodelocation_list(nodelocation_list)
  
  successlist, failurelist = run_parallelized(nodelocation_list, browse_node, identity)

  vesseldicts = []
  
  for (nodeid, vesseldicts_of_node) in successlist:
    vesseldicts += vesseldicts_of_node

  return _create_list_from_key_in_dictlist(vesseldicts, "vesselhandle")





def browse_node(nodelocation, identity):
  """
  <Purpose>
    Contact an individual node to gather detailed information about all of the
    vessels on the node that are usable by a given identity.
  <Arguments>
    nodelocation
      The nodelocation of the node that should be browsed. 
    identity
      The identity whose vessels we are interested in. This can be the identity
      of either the vessel owner or a vessel user.
  <Exceptions>
    NodeCommunicationError
      If the communication with the node fails for any reason, including the
      node not being reachable, timeout in communicating with the node, the
      node rejecting the 
  <Returns>
    A list of vesseldicts. Each vesseldict contains the additional keys:
      'status'
        The status string of the vessel.
      'ownerkey'
        The vessel's owner key (in dict format).
      'userkeys'
        A list of the vessel's user keys (each in dict format).
  """
  try:
    _validate_nodelocation(nodelocation)
    _validate_identity(identity)
    
    nmhandle = _get_nmhandle(nodelocation, identity)
    try:
      nodeinfo = nmclient_getvesseldict(nmhandle)
    except NMClientException, e:
      raise NodeCommunicationError("Failed to communicate with node " + nodelocation + ": " + str(e))
  
    # We do our own looking through the nodeinfo rather than use the function
    # nmclient_listaccessiblevessels() as we don't want to contact the node a
    # second time.
    usablevessels = []
    for vesselname in nodeinfo['vessels']:
      if identity['publickey_dict'] == nodeinfo['vessels'][vesselname]['ownerkey']:
        usablevessels.append(vesselname)
      elif 'userkeys' in nodeinfo['vessels'][vesselname] and \
          identity['publickey_dict'] in nodeinfo['vessels'][vesselname]['userkeys']:
        usablevessels.append(vesselname)
  
    nodeid = rsa_publickey_to_string(nodeinfo['nodekey'])
    # For efficiency, let's update the _node_location_cache with this info.
    # This can prevent individual advertise lookups of each nodeid by other
    # functions in the experimentlib that may be called later.
    _node_location_cache[nodeid] = nodelocation

    vesseldict_list = []
    for vesselname in usablevessels:
      vesseldict = {}
      # Required keys in vesseldicts (see the module comments for more info).
      vesseldict['vesselhandle'] = nodeid + ":" + vesselname
      vesseldict['nodelocation'] = nodelocation
      vesseldict['vesselname'] = vesselname
      vesseldict['nodeid'] = nodeinfo['nodekey']
      # Additional keys that browse_node provides.
      vesseldict['status'] = nodeinfo['vessels'][vesselname]['status']
      vesseldict['ownerkey'] = nodeinfo['vessels'][vesselname]['ownerkey']
      vesseldict['userkeys'] = nodeinfo['vessels'][vesselname]['userkeys']
      vesseldict_list.append(vesseldict)
  
    return vesseldict_list
  
  except Exception, e:
    # Useful for debugging during development of the experimentlib.
    #traceback.print_exc()
    raise





def _run_monitor(monitordict):
  """Performs the actual monitoring of vessels."""
  # Copy the vesselhandle_list so that changes made to the list can't happen
  # while the parallelized call is being done.
  vesselhandle_list = monitordict['vesselhandle_list'][:]
  
  run_parallelized(vesselhandle_list, _check_vessel_status_change, monitordict)
  
  # We finished the last run, now schedule another.
  monitordict['timerhandle'] = settimer(monitordict['waittime'], _run_monitor, [monitordict])





def _check_vessel_status_change(vesselhandle, monitordict):
  """
  Checks the status of an individual vessel and calls the registered
  callback function for the monitor if the vessel's status has changed since
  the last time it was checked.
  """
  try:
    # When the monitor is removed/canceled, the parallelized function isn't
    # aborted and we instead just have each of these calls immediately return.
    if monitordict['canceled']:
      return
    
    datadict = monitordict['vessels'][vesselhandle]
    if 'status' not in datadict:
      datadict['status'] = ''
      
    old_data = datadict.copy()
    
    status = get_vessel_status(vesselhandle, monitordict['identity'])
    datadict['status'] = status
    
    # No matter where the above try block returned from, we want to see if
    # the vessel data changed and call the user's callback if it has.
    new_data = datadict.copy()
    
    # Note that by not letting the lock go before we call the user's callback
    # function, the processing of all of the vessels will slow down but we
    # avoid requiring the user to handle locking to protect against another
    # call to the callback for the same vessel.
    if old_data['status'] != new_data['status']:
      try:
        # TODO: make sure that exception's from the user's code end up
        # somewhere where the user has access to them.
        monitordict['callback'](vesselhandle, old_data['status'], new_data['status'])
      
      except Exception:
        _debug_print("Exception occurred in vessel status change callback:")
        _debug_print(traceback.format_exc())
  
    # In order to prevent repeating failures, we remove the vesselhandle
    # from the monitor's list if the status indicates a positive response.
    # This means that scripts should occasionally add their known active
    # vessels to the monitor to prevent temporary failures from causing the
    # vessel to be subsequently ignored forever.
    if status in VESSEL_STATUS_SET_INACTIVE:
      _monitor_lock.acquire()
      try:
        monitordict['vesselhandle_list'].remove(vesselhandle)
        # We don't "del monitordict['vessels'][vesselhandle]" because it
        # doesn't hurt anything to leave it other than taking up a bit of
        # space, and it feels safer to leave it there just in case, for
        # example, this code got changed to put the "remove" call in the
        # try block above when access to the vessel's lock is still needed.
      finally:
        _monitor_lock.release()
      
  except Exception:
    _debug_print(traceback.format_exc())






def register_vessel_status_monitor(identity, vesselhandle_list, callback, waittime=300, concurrency=10):
  """
  <Purpose>
    Registers a vessel status monitor. Once registered, a monitor occassionally
    checks the status of each vessel. If the vessel's status has changed or was
    never checked before, the provided callback function is called with
    information about the status change.
  <Arguments>
    identity
      The identity to be used when looking checking vessel status. This is
      mostly needed to determine whether the vessel exists but no longer is
      usable by the identity (that is, if the public key of the identity is
      no longer neither the owner or a user of the vessel).
    vesselhandle_list
      A list of vesselhandles of the vessels to be monitored.
    callback
      The callback function. This should accept three arguments:
        (vesselhandle, oldstatus, newstatus)
      where oldstatus and newstatus are both strings.
    waittime
      How many seconds to wait between status checks. This will be the time
      between finishing a check of all vessels and starting another round of
      checking.
    concurrency
      The number of threads to use for communicating with nodes. This will be
      the maximum number of vessels that can be checked simultaneously.
  <Exceptions>
    None
  <Side Effects>
    Immediately starts a vessel monitor running.
  <Returns>
    A monitorhandle which can be used to update or cancel this monitor.
  """
  _validate_vesselhandle_list(vesselhandle_list)
  _validate_identity(identity)
  
  # We copy the vesselhandle_list so that the user doesn't directly modify.
  vesselhandle_list = vesselhandle_list[:]
  
  _monitor_lock.acquire()
  try:
    # Create a new monitor key in the the _vessel_monitors dict.
    for attempt in range(10):
      id = "MONITOR_" + str(random.random())
      if id not in _vessel_monitors:
        break
    else:
      raise Exception("Can't generate a unique vessel monitor id.")
    _vessel_monitors[id] = {}
    
    _vessel_monitors[id]['vesselhandle_list'] = vesselhandle_list
    _vessel_monitors[id]['waittime'] = waittime
    _vessel_monitors[id]['callback'] = callback
    _vessel_monitors[id]['concurrency'] = concurrency
    _vessel_monitors[id]['identity'] = identity
    # Whether the monitor was canceled/removed. Used to indicate to a running
    # monitor that it should stop doing work.
    _vessel_monitors[id]['canceled'] = False
    
    # Keeps track of the status of individual vessels. This is used by
    # vessel monitors to determine when the status has changed.
    _vessel_monitors[id]['vessels'] = {}
    for handle in vesselhandle_list:
      _vessel_monitors[id]['vessels'][handle] = {}
    
    # The first time we run it we don't delay. Storing the timer handle is a
    # bit useless in this case but we do it for consistency.
    _vessel_monitors[id]['timerhandle'] = settimer(0, _run_monitor, [_vessel_monitors[id]])
    
    return id
  
  finally:
    _monitor_lock.release()

  



def remove_vessel_status_monitor(monitorhandle):
  """
  <Purpose>
    Cancel a monitor that was created through register_vessel_status_monitor.
    Note that this will not terminate any already active run of the monitor
    (a run is a pass through contacting all relevant nodes to determine
    vessel status), but it will prevent future runs from starting.
  <Arguments>
    monitorhandle
      A monitorhandle returned by register_vessel_status_monitor.
  <Exceptions>
    ValueError
      If no such monitorhandle exists (including if it was already removed).
  <Side Effects>
    Stops future runs of the monitor and signals to any currently running
    monitor to stop. It is still possible that the registered callback for
    the monitor will be called after this function returns.
  <Returns>
    None
  """
  _monitor_lock.acquire()
  try:
    # Ensure the monitorhandle is valid.
    if not monitorhandle in _vessel_monitors:
      raise ValueError("The provided monitorhandle is invalid: " + str(monitorhandle))
    
    # Not using parallelize_abortfunction() because I didn't want to complicate
    # things by needing a way to get a hold of the parellelizehandle. Instead,
    # individual calls to _check_vessel_status_change will check if the monitor
    # was removed/canceled before doing any work.
    _vessel_monitors[monitorhandle]['canceled'] = True
    
    # Ignore the return value from canceltimer. If the user wants to ensure
    # that no further actions are taken in their own code due to this monitor,
    # they can do so by ignoring calls to their provided callback.
    canceltimer(_vessel_monitors[monitorhandle]['timerhandle'])
    
    del _vessel_monitors[monitorhandle]
    
  finally:
    _monitor_lock.release()





def add_to_vessel_status_monitor(monitorhandle, vesselhandle_list):
  """
  <Purpose>
    Adds the vesselhandles in vesselhandle_list to the specified monitor. If
    any already are watched by the monitor, they are silently ignored. There
    is no removal of previously added vesselhandles other than automatic
    removal done when vessels are unreachable or otherwise invalid.
    
    One intention of this function is that new vessels found via a
    lookup_node_locations_by_identity and then find_vessels_on_nodes can be
    passed to this function as a way of making sure the monitor knows about
    new vessels that a user has just obtained access to or which have recently
    come online. 
  <Arguments>
    monitorhandle
      A monitorhandle returned by register_vessel_status_monitor.
    vesselhandle_list
      A list of vesselhandles to add to the monitor.
  <Side Effects>
    The next run of the monitor will include the provided vesselhandles.
  <Exceptions>
    ValueError
      If no such monitorhandle exists (including if it was already removed).
  <Returns>
    None
  """
  _validate_vesselhandle_list(vesselhandle_list)
  
  _monitor_lock.acquire()
  try:
    # Ensure the monitorhandle is valid.
    if not monitorhandle in _vessel_monitors:
      raise ValueError("The provided monitorhandle is invalid: " + str(monitorhandle))
    
    for vesselhandle in vesselhandle_list:
      if vesselhandle not in _vessel_monitors[monitorhandle]['vesselhandle_list']:
        _vessel_monitors[monitorhandle]['vesselhandle_list'].append(vesselhandle)
        _vessel_monitors[monitorhandle]['vessels'][vesselhandle] = {}
    
  finally:
    _monitor_lock.release()





def get_vessel_status(vesselhandle, identity):
  """
  <Purpose>
    Determine the status of a vessel.
  <Arguments>
    vesselhandle
      The vesselhandle of the vessel whose status is to be checked.
    identity
      The identity of the owner or a user of the vessel.
  <Exceptions>
    UnexpectedVesselStatusError
      If the status returned by the node for the vessel is not a status value
      that this experimentlib expects.
  <Side Effects>
    The node the vessel is on is communicated with.
  <Returns>
    A string that is one of the VESSEL_STATUS_* constants.
  """
  _validate_vesselhandle(vesselhandle)
  _validate_identity(identity)
    
  # Determine the last known location of the node. 
  nodeid, vesselname = vesselhandle.split(":")
  try:
    # This will get a cached node location if one exists.
    nodelocation = get_node_location(nodeid)
  except NodeLocationLookupError, e:
    # If we can't find the node, then it must not be advertising.
    _debug_print("get_vessel_status cannot determine location of node: " + str(e))
    return VESSEL_STATUS_NO_SUCH_NODE
  
  try:
    # TODO: we need to also detect whether the vessel no longer belongs to the
    # user. Shouldn't browse_node take care of this by only returning vessels
    # that this identity is the owner or a user of?
    vesselinfolist = browse_node(nodelocation, identity)
  except NodeCommunicationError:
    # Do a non-cache lookup of the nodeid to see if the node moved.
    try:
      nodelocation = get_node_location(nodeid, ignorecache=True)
    except NodeLocationLookupError, e:
      return VESSEL_STATUS_NO_SUCH_NODE

    # Try to communicate again.
    try:
      vesselinfolist = browse_node(nodelocation, identity)
    except NodeCommunicationError, e:
      return VESSEL_STATUS_NODE_UNREACHABLE

  for vesselinfo in vesselinfolist:
    if vesselinfo['vesselhandle'] == vesselhandle:
      # The node is up and the vessel must have the identity's key as the owner
      # or a user, but the status returned isn't one of the statuses we
      # expect. If this does occur, it may indicate a bug in the experiment
      # library where it doesn't know about all possible status a nodemanager
      # may return for a vessel.
      if vesselinfo['status'] not in VESSEL_STATUS_SET_ACTIVE:
        raise UnexpectedVesselStatusError(vesselinfo['status'])
      else:
        return vesselinfo['status']
  else:
    # The node is up but this vessel doesn't exist.
    return VESSEL_STATUS_NO_SUCH_VESSEL
      




def _do_public_node_request(nodeid, requestname, *args):
  nodelocation = get_node_location(nodeid)
  nmhandle = _get_nmhandle(nodelocation)
  
  try:
    return nmclient_rawsay(nmhandle, requestname, *args)
  except NMClientException, e:
    raise NodeCommunicationError(str(e))





def _do_signed_vessel_request(identity, vesselhandle, requestname, *args):
  _validate_identity(identity, require_private_key=True)
  
  nodeid, vesselname = vesselhandle.split(':')
  nodelocation = get_node_location(nodeid)
  nmhandle = _get_nmhandle(nodelocation, identity)
  
  try:
    return nmclient_signedsay(nmhandle, requestname, vesselname, *args)
  except NMClientException, e:
    raise NodeCommunicationError(str(e))





def get_offcut_resources(nodeid):
  """
  <Purpose>
    Obtain information about offcut resources on a node.
  <Arguments>
  <Exceptions>
  <Side Effects>
  <Returns>
    A string containing information about the node's offcut resources.
  """
  return _do_public_node_request(nodeid, "GetOffcutResources")
  




def get_restrictions_info(nodeid):
  """
  <Purpose>
    Obtain vessel resource/restrictions information.
  <Arguments>
  <Exceptions>
  <Side Effects>
  <Returns>
    A string containing the vessel resource/restrictions information.
  """
  return _do_public_node_request(nodeid, "GetVesselResources")
  




def get_log(vesselhandle, identity):
  """
  <Purpose>
    Read the vessel log.
  <Arguments>
  <Exceptions>
  <Side Effects>
  <Returns>
    A string containing the data in the vessel log.
  """
  _validate_vesselhandle(vesselhandle)
  return _do_signed_vessel_request(identity, vesselhandle, "ReadVesselLog")




def get_file_list(vesselhandle, identity):
  """
  <Purpose>
    Get a list of files that are on the vessel.
  <Arguments>
  <Exceptions>
  <Side Effects>
  <Returns>
    A list of filenames (strings).
  """
  _validate_vesselhandle(vesselhandle)
  file_list_string = _do_signed_vessel_request(identity, vesselhandle, "ListFilesInVessel")
  return file_list_string.split(' ')




def upload_file(vesselhandle, identity, local_filename, remote_filename=None):
  """
  <Purpose>
    Upload a file to a vessel.
  <Arguments>
    vesselhandle
    identity
    local_filename
    remote_filename
      (optional) The filename to use when storing the file on the vessel. If
      not provided, this will be the same as the basename of local_filename.
      Note that the remote_filename is subject to filename restrictions imposed
      on all vessels.
      TODO: describe the filename restrictions.
  <Exceptions>
  <Side Effects>
  <Returns>
    The full filename where this file was ultimately saved to. This will be in
    the current working directory unless local_filename_prefix included a path
    to a different directory.
  """
  _validate_vesselhandle(vesselhandle)
  
  if remote_filename is None:
    remote_filename = os.path.basename(local_filename)

  fileobj = open(local_filename, "r")
  filedata = fileobj.read()
  fileobj.close()
  
  _do_signed_vessel_request(identity, vesselhandle, "AddFileToVessel", remote_filename, filedata)





def download_file(vesselhandle, identity, remote_filename, local_filename=None,
                  add_location_suffix=False, return_file_contents=False):
  """
  <Purpose>
    Download a file from a vessel.
  <Arguments>
    vesselhandle
    identity
    remote_filename
    local_filename
      (optional) The filename to use when saving the downloaded file locally.
      This can include a directory path.
    add_location_suffix
      (optional) Whether the nodelocation and vesselname should be suffixed to
      the end of the local filename when saving the file.
    local_filename
      (optional) If True, the downloaded file will not be saved locally and
      instead will be returned as a string instead of the local filename.
  <Exceptions>
  <Side Effects>
  <Returns>
    If return_file_contents is False:
      The full filename where this file was ultimately saved to. This will be in
      the current working directory unless local_filename_prefix included a path
      to a different directory.
    If return_file_contents is True:
      The contents of the remote file as a string.
  """
  _validate_vesselhandle(vesselhandle)
  
  if not return_file_contents:
    if local_filename is None:
      local_filename = remote_filename
    if add_location_suffix:
      nodeid, vesselname = vesselhandle.split(':')
      nodelocation = get_node_location(nodeid)
      suffix = "_".join(nodelocation.split(':') + [vesselname])
      local_filename += "_" + suffix
  
  retrieveddata = _do_signed_vessel_request(identity, vesselhandle, "RetrieveFileFromVessel", remote_filename)
  
  if return_file_contents:
    return retrieveddata
  else:
    fileobj = open(local_filename, "w")
    fileobj.write(retrieveddata)
    fileobj.close()
    return local_filename





def delete_file(vesselhandle, identity, filename):
  """
  <Purpose>
    Delete a file from a vessel.
 """
  _validate_vesselhandle(vesselhandle)
  _do_signed_vessel_request(identity, vesselhandle, "DeleteFileInVessel", filename)





def reset_vessel(vesselhandle, identity):
  """
  <Purpose>
    Stop the vessel if it is running and reset it to a fresh state. This will
    delete all files from the vessel.
  """
  _validate_vesselhandle(vesselhandle)
  _do_signed_vessel_request(identity, vesselhandle, "ResetVessel")





def start_vessel(vesselhandle, identity, program_file, arg_list):
  """
  <Purpose>
    Start a program running on a vessel.
  """
  _validate_vesselhandle(vesselhandle)
  arg_list.insert(0, program_file)
  arg_string = " ".join(arg_list)
  _do_signed_vessel_request(identity, vesselhandle, "StartVessel", arg_string)
  





def stop_vessel(vesselhandle, identity):
  """
  <Purpose>
    Stop the currently running program on a vessel, if there is one.
  """
  _validate_vesselhandle(vesselhandle)
  _do_signed_vessel_request(identity, vesselhandle, "StopVessel")



  
  
def split_vessel(identity, vesselhandle, resourcedata):
  """
  <Purpose>
    Split a vessel into two new vessels.
    
    THIS OPERATION IS ONLY AVAILABLE TO THE OWNER OF THE VESSEL.
    If you have acquired the vessel through SeattleGENI, you are a user of the
    vessel, not an owner.
  """
  _validate_vesselhandle(vesselhandle)
  _do_signed_vessel_request(identity, vesselhandle, "SplitVessel", resourcedata)





def combine_vessels(identity, vesselhandle1, vesselhandle2):
  """
  <Purpose>
    Join a two vessels on the same node into one, larger vessel.
    
    THIS OPERATION IS ONLY AVAILABLE TO THE OWNER OF THE VESSEL.
    If you have acquired the vessel through SeattleGENI, you are a user of the
    vessel, not an owner.
  """
  _validate_vesselhandle(vesselhandle1)
  _validate_vesselhandle(vesselhandle2)
  vesselname2 = vesselhandle2.split(":")[1]
  _do_signed_vessel_request(identity, vesselhandle1, "JoinVessels", vesselname2)





def set_vessel_owner(vesselhandle, identity, new_owner_identity):
  """
  <Purpose>
    Change the owner of a vessel.
    
    THIS OPERATION IS ONLY AVAILABLE TO THE OWNER OF THE VESSEL.
    If you have acquired the vessel through SeattleGENI, you are a user of the
    vessel, not an owner.
  """
  _validate_vesselhandle(vesselhandle)
  _do_signed_vessel_request(identity, vesselhandle, "ChangeOwner", new_owner_identity['publickey_str'])
  
  



def set_vessel_advertise(vesselhandle, identity, advertise_enabled):
  """
  <Purpose>
    Set whether the vessel should be advertising or not.
    
    THIS OPERATION IS ONLY AVAILABLE TO THE OWNER OF THE VESSEL.
    If you have acquired the vessel through SeattleGENI, you are a user of the
    vessel, not an owner.
  """
  _validate_vesselhandle(vesselhandle)
  
  if not isinstance(advertise_enabled, bool):
    raise TypeError("advertise_enabled must be a boolean.")

  _do_signed_vessel_request(identity, vesselhandle, "ChangeAdvertise", str(advertise_enabled))
  
  



def set_vessel_ownerinfo(vesselhandle, identity, ownerinfo):
  """
  <Purpose>
    Set the owner info of a vessel.
    
    THIS OPERATION IS ONLY AVAILABLE TO THE OWNER OF THE VESSEL.
    If you have acquired the vessel through SeattleGENI, you are a user of the
    vessel, not an owner.
  """
  _validate_vesselhandle(vesselhandle)
  _do_signed_vessel_request(identity, vesselhandle, "ChangeOwnerInformation", ownerinfo)


  


def set_vessel_users(vesselhandle, identity, userkeystringlist):
  """
  <Purpose>
    Change the owner of a vessel.
    
    THIS OPERATION IS ONLY AVAILABLE TO THE OWNER OF THE VESSEL.
    If you have acquired the vessel through SeattleGENI, you are a user of the
    vessel, not an owner.
  <Arguments>
    vesselhandle
    identity
    userkeystringlist
      A list of key strings. The key strings must be in the format of the data
      stored in key files. That is, each should be a string that consists of
      the modulus, followed by a space, followed by the public exponent.
  """
  _validate_vesselhandle(vesselhandle)
  formatteduserkeys = '|'.join(userkeystringlist)
  _do_signed_vessel_request(identity, vesselhandle, "ChangeUsers", formatteduserkeys)
  




def get_nodeid_and_vesselname(vesselhandle):
  """
  <Purpose>
    Given a vesselhandle, returns the nodeid and vesselname.
  """
  _validate_vesselhandle(vesselhandle)
  return vesselhandle.split(":")





def get_host_and_port(nodelocation):
  """
  <Purpose>
    Given a nodelocation, returns a (string) host and (integer) port. The host
    may be an IP address or an identifier used by NAT forwarders.
  """
  _validate_nodelocation(nodelocation)
  host, portstr = nodelocation.split(":")
  return host, int(portstr)





def get_node_location(nodeid, ignorecache=False):
  """
  <Purpose>
    Determine a nodelocation given a nodeid.
  <Arguments>
    nodeid
      The nodeid of the node whose location is to be determined.
    ignorecache
      (optional, default is False) Whether to ignore cached values for this
      node's location, forcing an advertise lookup and possibly also
      attempting to contact potential nodelocations.
  <Exceptions>
    NodeLocationLookupError
      If the location of the node with nodeid cannot be determined.
  <Side Effects>
    If the node location isn't already known (or if ignorecache is True),
    then an advertise lookup of the nodeid is done. In that case, if
    multiple nodelocations are advertised under the nodeid, then each location
    will be contacted until one is determined to be a valid nodelocation
    that can be communicated with.
  <Returns>
    A nodelocation.
  """
  if ignorecache or nodeid not in _node_location_cache:
    locationlist = lookup_node_locations_by_nodeid(nodeid)
    if not locationlist:
      raise NodeLocationLookupError("Nothing advertised under node's key.")
    # If there is more than one advertised location, we need to figure out
    # which one is valid. For example, if a node moves then there will be
    # a period of time in which the old advertised location and the new
    # one are both returned. We need to determine the correct one.
    elif len(locationlist) > 1:
      for possiblelocation in locationlist:
        host, portstr = possiblelocation.split(':')
        try:
          # We create an nmhandle directly because we want to use it to test
          # basic communication, which is done when an nmhandle is created.
          nmhandle = nmclient_createhandle(host, int(portstr))
        except NMClientException, e:
          continue
        else:
          nmclient_destroyhandle(nmhandle)
          _node_location_cache[nodeid] = possiblelocation
          break
      else:
        raise NodeLocationLookupError("Multiple node locations advertised but none " + 
                                      "can be communicated with: " + str(locationlist))
    else:
      _node_location_cache[nodeid] = locationlist[0]
      
  return _node_location_cache[nodeid]
    
  



  
  
def _call_seattlegeni_func(func, *args, **kwargs):
  """
  Helper function to limit the potential errors raised by seattlegeni_*
  functions to SeattleGENIError or classes that extend it. The seattlegeni_xmlrpc
  module doesn't catch ProtocolError or unexpected xmlrpc faults. At the level
  of the experimentlib, though, we just consider these generic failures for the
  purpose of simlifying error handling when using the experimentlib.
  """
  try:
    return func(*args, **kwargs)
  except xmlrpclib.ProtocolError:
    raise SeattleGENIError("Failed to communicate with SeattleGENI. " +
                           "Are you using the correct xmlrpc url? " + traceback.format_exc())
  except xmlrpclib.Fault:
    raise SeattleGENIError("Unexpected XML-RPC fault when talking to SeattleGENI. " +
                           "Are you using a current version of experimentlib.py and " +
                           "seattlegeni_xmlrpc.py? " + traceback.format_exc())

  



def _get_seattlegeni_client(identity):
  
  if "seattlegeniclient" not in identity:
    _validate_identity(identity, require_private_key=True, require_username=True)
    private_key_string = rsa_privatekey_to_string(identity["privatekey_dict"])
    # We use _call_seattlegeni_func because the SeattleGENIClient constructor
    # may attempt to communicate with SeattleGENI.
    client = _call_seattlegeni_func(seattlegeni_xmlrpc.SeattleGENIClient,
                                    identity['username'],
                                    private_key_string=private_key_string,
                                    xmlrpc_url=SEATTLEGENI_XMLRPC_URL,
                                    allow_ssl_insecure=SEATTLEGENI_ALLOW_SSL_INSECURE,
                                    ca_certs_file=SEATTLEGENI_CA_CERTS_FILES)
    identity["seattlegeniclient"] = client
    
  return identity["seattlegeniclient"]

  
  

  
def _seattlegeni_cache_node_locations(seattlegeni_vessel_list):
  """
  This takes a list of vessel dicts that aren't the standard vesseldict this
  module normally deals with. Instead, these are dicts with the keys that are
  directly returned by the seattlegeni xmlrpc api.
  """
  for seattlegeni_vessel in seattlegeni_vessel_list:
    nodeid = seattlegeni_vessel['node_id']
    ip = seattlegeni_vessel['node_ip']
    portstr = str(seattlegeni_vessel['node_port'])
    _node_location_cache[nodeid] = ip + ':' + portstr





def seattlegeni_acquire_vessels(identity, type, number):
  """
  <Purpose>
    Acquire vessels of a certain type from SeattleGENI. This is an
    all-or-nothing request. Either the number requested will be acquired or
    no vessels will be acquired.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
    type
      The type of vessels to be acquired. This must be one of the constants
      named SEATTLEGENI_VESSEL_TYPE_*
    number
      The number of vessels to be acquired.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments, as well as:
    SeattleGENINotEnoughCreditsError
      If the account does not have enough available vessel credits to fulfill
      the request.
  <Side Effects>
    Either the full number of vessels requested are acquired or none are.
  <Returns>
    A list of vesselhandles of the acquired vessels.
  """
  client = _get_seattlegeni_client(identity)
  seattlegeni_vessel_list = _call_seattlegeni_func(client.acquire_resources, type, number)

  _seattlegeni_cache_node_locations(seattlegeni_vessel_list)
  
  return _create_list_from_key_in_dictlist(seattlegeni_vessel_list, "handle")





def seattlegeni_acquire_specific_vessels(identity, vesselhandle_list):
  """
  <Purpose>
    Acquire specific vessels from SeattleGENI. This is not an all-or-nothing
    request.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
    vesselhandle_list
      A list of vesselhandles. Even though the request may be only partially
      fulfilled, the size of this list must not be greater than the number of
      vessels the account has available to acquire.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments, as well as:
    SeattleGENINotEnoughCreditsError
      If the account does not have enough available vessel credits to fulfill
      the request.
  <Side Effects>
    If successful, zero or more vessels from handlelist have been acquired.
  <Returns>
    A list of vesselhandles of the acquired vessels.
  """
  client = _get_seattlegeni_client(identity)
  seattlegeni_vessel_list = _call_seattlegeni_func(client.acquire_specific_vessels, type, vesselhandle_list)
  
  _seattlegeni_cache_node_locations(seattlegeni_vessel_list)
  
  return _create_list_from_key_in_dictlist(seattlegeni_vessel_list, "handle")





def seattlegeni_release_vessels(identity, vesselhandle_list):
  """
  <Purpose>
    Release vessels from SeattleGENI.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
    vesselhandle_list
      The vessels to be released.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments.
  <Side Effects>
    The vessels are released from the SeattleGENI account.
  <Returns>
    None
  """
  _validate_vesselhandle_list(vesselhandle_list)
  
  client = _get_seattlegeni_client(identity)
  _call_seattlegeni_func(client.release_resources, vesselhandle_list)





def seattlegeni_renew_vessels(identity, vesselhandle_list):
  """
  <Purpose>
    Renew vessels previously acquired from SeattleGENI.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
    vesselhandle_list
      The vessels to be renewed.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments, as well as:
    SeattleGENINotEnoughCredits
      If the account is currently over its vessel credit limit, then vessels
      cannot be renewed until the account is no longer over its credit limit.
  <Side Effects>
    The expiration time of the vessels is is reset to the maximum.
  <Returns>
    None
  """
  _validate_vesselhandle_list(vesselhandle_list)
  
  client = _get_seattlegeni_client(identity)
  _call_seattlegeni_func(client.renew_vessels, vesselhandle_list)





def seattlegeni_get_acquired_vessels(identity):
  """
  <Purpose>
    Obtain a list of vesselhandles corresponding to the vessels acquired through
    SeattleGENI.
  
    In order to return a data format that is most useful with the other functions
    in this module, this function drops some potentially useful info. Therefore,
    there's a separate function:
      seattlegeni_get_acquired_vessels_details()
    for obtaining all of the vessel information returned by seattlegeni.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments.
  <Side Effects>
    None
  <Returns>
    A list of vesselhandles.
  """  
  vesseldict_list = seattlegeni_get_acquired_vessels_details(identity)

  # We look for the vesselhandle key rather than 'handle' because these
  # are vesseldicts, by our definition of them, not the raw dictionaries
  # that seattlegeni hands back. 
  return _create_list_from_key_in_dictlist(vesseldict_list, "vesselhandle")





def seattlegeni_get_acquired_vessels_details(identity):
  """
  <Purpose>
    Obtain a list of vesseldicts corresponding to the the vessels acquired
    through SeattleGENI.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments.
  <Side Effects>
    None
  <Returns>
    A list of vesseldicts that have the additional key 'expires_in_seconds'.
  """  
  client = _get_seattlegeni_client(identity)
  seattlegeni_vessel_list = _call_seattlegeni_func(client.get_resource_info)
  
  _seattlegeni_cache_node_locations(seattlegeni_vessel_list)

  # Convert these dicts into dicts that have the required keys for us to
  # consider them "vesseldicts", by the definition given in the module
  # comments.
  vesseldict_list = []
  for seattlegeni_vessel in seattlegeni_vessel_list:
    vesseldict = {}
    vesseldict_list.append(vesseldict)

    nodeid = seattlegeni_vessel['node_id']
    ip = seattlegeni_vessel['node_ip']
    portstr = str(seattlegeni_vessel['node_port'])
    vesselname = seattlegeni_vessel['vessel_id']
    
    # Required keys in vesseldicts (see the module comments for more info).
    vesseldict['vesselhandle'] = nodeid + ":" + vesselname
    vesseldict['nodelocation'] = ip + ':' + portstr
    vesseldict['vesselname'] = vesselname
    vesseldict['nodeid'] = nodeid
    # Additional keys that browse_node provides.
    vesseldict['expires_in_seconds'] = seattlegeni_vessel['expires_in_seconds']

  return vesseldict_list





def seattlegeni_max_vessels_allowed(identity):
  """
  <Purpose>
    Determine the maximum number of vessels that can be acquired by this
    account through SeattleGENI, regardless of the number currently acquired.
    That is, this is an absolute maximum, not the number that can still be
    acquired based on the number already acquired.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments.
  <Side Effects>
    None
  <Returns>
    The maximum number of vessels the account can acquire (an integer).
  """  
  client = _get_seattlegeni_client(identity)
  return _call_seattlegeni_func(client.get_account_info)['max_vessels']





def seattlegeni_user_port(identity):
  """
  <Purpose>
    Determine the port which SeattleGENI guarantees will be usable by the
    account on all acquired vessels.
  <Arguments>
    identity
      The identity to use for communicating with SeattleGENI.
  <Exceptions>
    The common SeattleGENI exceptions described in the module comments.
  <Side Effects>
    None
  <Returns>
    The port number (an integer).
  """  
  client = _get_seattlegeni_client(identity)
  return _call_seattlegeni_func(client.get_account_info)['user_port']
