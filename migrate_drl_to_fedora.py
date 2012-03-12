#!/usr/local/bin/python
import sys
import os
import logging
import zipfile
import subprocess
import shutil
sys.path.append('/usr/local/dlxs/prep/w/workflow/lib')
sys.path.append('/usr/local/dlxs/prep/w/workflow/django')
os.environ["DJANGO_SETTINGS_MODULE"] = 'workflow.settings'
import workflow.core.models
import workflow.wflocal.models
import drl.utils 
import fcrepo.connection
import fcrepo.client
from islandoraUtils import fileConverter as converter
from islandoraUtils import fedoraLib
from islandoraUtils.metadata import fedora_relationships
from utils.commonFedora import addObjectToFedora
"""
Utility script to migrate digital objects from the legacy (as of 2012)
DRL repository to Fedora.

For each object, determine content
type and route to a handler function that will create the object in 
Fedora and upload all relevant datastreams.
"""

logging.basicConfig(level=logging.DEBUG)

# map of legacy collection ids to fedora namespace
COLL_NS_MAP = {
    'pitttext': 'hpitt',
    'pitttextdir': 'hpitt',
    'hpicasc': 'hpitt',
    'hpichswp': 'hpitt',
    'hpiccma': 'hpitt',
    'hpicchatham': 'hpitt',
    'hpicpointpark': 'hpitt',
    'hpicoakmont': 'hpitt',
    'hopkins': 'hpitt',
    'flood': 'hpitt',
    'warrantee': 'hpitt',
    'geotopo': 'hpitt',
}

# map of legacy item types to fedora content models
ITEM_TYPE_CM_MAP = {
    'image': 'islandora:sp_large_image_cmodel',
    'text - cataloged': 'islandora:bookCModel',
    'text - uncataloged': 'islandora:bookCModel',
    'map': 'islandora:sp_large_image_cmodel',
    'manuscript': 'islandora:bookCModel',
    'page': 'islandora:pageCModel'
}


def connect_to_fedora():
    try:
        connection = fcrepo.connection.Connection('http://akira.library.pitt.edu:8080/fedora', username='xxxxx', password='xxxxx')
    except Exception as ex:
        print 'Error while connecting to fedora: %s' % (ex.message,)
        return None

    try:
        return fcrepo.client.FedoraClient(connection)
    except Exception, ex:
        print 'Exception while opening fedora client: %s' % (ex.message,)
    return None

def get_collection_members(collection_id):
    return workflow.core.models.Item.objects.filter(primary_collection__c_id=collection_id)

def handle_base_object(fedora_client, item, ns, cm):
    """Create the base object record in Fedora, add common datastreams.

    @param item: The django item object from legacy workflow
    @param ns: The namespace to be used for the object's pid
    @param cm: The pid of the content model to be associated with the object

    Other required data fields come from the item record itself: pid, label

    """
    print '%s - handle base object' % (item.do_id,)
    parent_pid = '%s:root' % (ns,)
    pid = '%s:%s' % (ns, item.do_id)
    label = drl.utils.shorten_string(item.name, 245)
    # if this object already exists, return (for now)
    try:
        obj = fedora_client.getObject(pid)
        return None
    except:
        pass
    # validate required objects, (for now) skip if not found
    try:
        mods = workflow.core.models.Item_File.objects.get(item=item, use='MODS')
        dc = workflow.core.models.Item_File.objects.get(item=item, use='DC')
        thumb = workflow.core.models.Item_File.objects.get(item=item, use='THUMB')
    except:
        return
    try:
        obj = addObjectToFedora(fedora_client, label, pid, parent_pid, cm)
        logging.info('added object to fedora OK')
    except Exception, ex:
        print 'connection error while trying to add fedora object %s: %s' % (pid, ex.message)
        return False
    # mods
    mods = workflow.core.models.Item_File.objects.get(item=item, use='MODS')
    logging.info('adding MODS datastream')
    fedoraLib.update_datastream(obj, u'MODS', mods.path, label=mods.name, mimeType=u'text/xml', controlGroup='X')
    # dc
    dc = workflow.core.models.Item_File.objects.get(item=item, use='DC')
    logging.info('adding DC datastream')
    fedoraLib.update_datastream(obj, u'DC', dc.path, label=dc.name, mimeType=u'text/xml', controlGroup='M')
    # thumb
    thumb = workflow.core.models.Item_File.objects.get(item=item, use='THUMB')
    logging.info('adding thumbnail datastream')
    fedoraLib.update_datastream(obj, u'TN', thumb.path, label=thumb.name, mimeType=u'image/jpeg', controlGroup='M')
    return obj

def handle_derived_jp2(fedora_object, tiff):
    baseName = os.path.splitext(tiff.name)[0]
    #jp2_file = os.path.join('/tmp', '%s.jp2' % (baseName,))
    #converter.tif_to_jp2(tiff.path, jp2_file, 'default', 'default')
    shutil.copy(tiff.path, '/tmp/')
    jp2_source = os.path.join('/tmp', tiff.name)
    encoder = '/usr/local/dlxs/prep/i/image/encodeJp2'
    jp2_file = subprocess.Popen([encoder, jp2_source], stdout=subprocess.PIPE).communicate()[0].strip()
    os.remove(jp2_source)
    fedoraLib.update_datastream(fedora_object, u"JP2", jp2_file, label=os.path.basename(jp2_file), mimeType=u'image/jp2', controlGroup='M')
    os.remove(jp2_file) # finished with that
    return
        
def handle_image_object(fedora_object, item):
    print '%s - handle image object' % (item.do_id,)
    # tiff image file
    tiff = workflow.core.models.Item_File.objects.get(item=item, use='MASTER')
    fedoraLib.update_datastream(fedora_object, 'TIFF', tiff.path, label=tiff.name, mimeType='image/tiff', controlGroup='M')
    handle_derived_jp2(fedora_object, tiff)
    return 

def handle_text_object(fedora_client, fedora_object, item):
    print '%s - handle text object' % (item.do_id,)
    # marcxml
    marcxml = workflow.core.models.Item_File.objects.get(item=item, use='MARCXML')
    logging.info('adding MARCXML datastream')
    fedoraLib.update_datastream(fedora_object, u'MARCXML', marcxml.path, label=marcxml.name, mimeType=u'text/xml', controlGroup='M')
    # mets 
    mets = workflow.core.models.Item_File.objects.get(item=item, use='METS')
    logging.info('adding METS datastream')
    fedoraLib.update_datastream(fedora_object, u'METS', mets.path, label=mets.name, mimeType=u'text/xml', controlGroup='M')
    # ocr zip
    ocr_zipfile = workflow.core.models.Item_File.objects.get(item=item, use='OCR_ZIP')
    ocr_zip = zipfile.ZipFile(ocr_zipfile.path, 'r')
    # pages
    pages = workflow.core.models.Item_File.objects.filter(item=item, use='MASTER').order_by('name')
    page_cm = 'archiveorg:pageCModel'
    for page in pages:
        page_basename = os.path.splitext(page.name)[0]
        page_pid = '%s-%s' % (fedora_object.pid, page_basename)
        page_label = u'%s-%s' % (fedora_object.label, page_basename)
        extraNamespaces = { 'pageNS' : 'info:islandora/islandora-system:def/pageinfo#' }
        extraRelationships = { fedora_relationships.rels_predicate('pageNS', 'isPageNumber') : str(int(page_basename)) }
        page_object = addObjectToFedora(fedora_client, page_label, page_pid, fedora_object.pid, page_cm, extraNamespaces=extraNamespaces, extraRelationships=extraRelationships)
        fedoraLib.update_datastream(page_object, 'TIFF', page.path, label=page.name, mimeType='image/tiff', controlGroup='M')
        handle_derived_jp2(page_object, page)
        ocr_filename = '%s.txt' % (page_basename,)
        if ocr_filename in ocr_zip.namelist():
            ocr_file = ocr_zip.extract(ocr_filename, '/tmp') 
            ocr_path = os.path.join('/tmp', ocr_filename) 
            fedoraLib.update_datastream(page_object, u'OCR', ocr_path, label=unicode(ocr_filename), mimeType=u'text/plain', controlGroup='M')
            os.remove(ocr_path)
    return

def handle_map_object(fedora_object, item):
    print '%s - handle map object' % (item.do_id,)
    # tiff image file
    tiff = workflow.core.models.Item_File.objects.get(item=item, use='MASTER')
    fedoraLib.update_datastream(fedora_object, 'TIFF', tiff.path, label=tiff.name, mimeType='image/tiff', controlGroup='M')
    handle_derived_jp2(fedora_object, tiff)
    return

def handle_manuscript_object(fedora_object, item):
    print '%s - handle manuscript object' % (item.do_id,)
    # mets
    # for each page:
        # tiff page image file
        # dc

def get_collection_namespace(item):
    return COLL_NS_MAP[item.primary_collection.c_id] 

def get_item_content_model(item):
    return ITEM_TYPE_CM_MAP[item.type.name]

def ingest_collection(collection_id):
    for item in get_collection_members(collection_id):
        ingest_item(item)

def ingest_item(item):
    logging.info('connecting to fedora')
    fedora_client = connect_to_fedora()
    if not fedora_client:
        sys.exit(0)        
    logging.info('connected to fedora')
    ns = get_collection_namespace(item)
    print '%s - coll namespace: %s' % (item.do_id, ns)
    cm = get_item_content_model(item)
    print '%s - content model: %s' % (item.do_id, cm)
    type = item.type.name
    logging.info('handling basic object setup')
    fedora_object = handle_base_object(fedora_client, item, ns, cm)
    if not fedora_object:
        return
    if type == 'image':
        handle_image_object(fedora_object, item)
    elif type == 'text - uncataloged' or type == 'text - cataloged':
        handle_text_object(fedora_client, fedora_object, item)
    elif type == 'map':
        handle_map_object(fedora_object, item)
    elif type == 'manuscript':
        handle_manuscript_object(fedora_object, item)
    else:
        pass

    
    
    
