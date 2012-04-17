#!/usr/local/bin/python
import sys
import os
import zipfile
import subprocess
import shutil
from lxml import etree 
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

def clean_page_labels(dict):
    cleaned_dict = {}
    for file in dict.keys():
        label = dict[file]
        if label == 'unum':
            seq = os.path.splitext(file)[0]
            cleaned_dict[file] = '[unnumbered page (%s)]' % (seq,)
        elif label.startswith('r0'):
            digit = int(label[1:4])
            roman = drl.utils.get_roman_numeral(digit)
            cleaned_dict[file] = 'Page %s' % (roman,)
        else:
            try:
                i = int(label)
                cleaned_dict[file] = 'Page %s' % (i,)
            except:
                cleaned_dict[file] = '[page %s]' % (label,)
    return cleaned_dict 
            

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

def get_collection_namespace(item):
    return COLL_NS_MAP[item.primary_collection.c_id] 

def get_item_content_model(item):
    return ITEM_TYPE_CM_MAP[item.type.name]

def get_page_label_dict_from_mets(mets_path):
    """
    Parse the METS structMap to get proper page label
    """
    METS_NS_MAP = {'mets': 'http://www.loc.gov/METS/'} 
    mets = etree.parse(open(mets_path, 'r'))
    labels = {}
    for file in mets.iter('{http://www.loc.gov/METS/}file'):
        file_id = file.get('ID')
        file_name = file[0].get('{http://www.w3.org/1999/xlink}href')
        xpath_string = '//mets:div[@TYPE="page"]/mets:fptr[@FILEID="%s"]' % (file_id,)
        fptr = mets.xpath(xpath_string, namespaces=METS_NS_MAP)[0]
        label = fptr.getparent().get('LABEL')
        labels[file_name] = label
    return labels

def handle_base_object(fedora_client, item, ns, cm):
    """
    Create the base object record in Fedora, add common datastreams.

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
        return obj 
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
    except Exception, ex:
        print 'connection error while trying to add fedora object %s: %s' % (pid, ex.message)
        return False
    # mods
    mods = workflow.core.models.Item_File.objects.get(item=item, use='MODS')
    fedoraLib.update_datastream(obj, u'MODS', mods.path, label=mods.name, mimeType=u'text/xml', controlGroup='X')
    # dc
    dc = workflow.core.models.Item_File.objects.get(item=item, use='DC')
    fedoraLib.update_datastream(obj, u'DC', dc.path, label=dc.name, mimeType=u'text/xml', controlGroup='M')
    # thumb
    thumb = workflow.core.models.Item_File.objects.get(item=item, use='THUMB')
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

"""
Note: this converter is not finished yet
"""
def handle_derived_pdf(fedora_object, tiff):
    """
    Create pdf derivative from tiff, not sure if the pyutils converter is the way to go
    or if /usr/local/dlxs contains an encoder for this
    """
    baseName = os.path.splitext(tiff.name)[0]
    pdf_file = os.path.join("/tmp", "%s.pdf" % baseName)
    #converter.tif_to_pdf(tiff, pdf_file, 'default')
    #fedoraLib.update_datastream(fedora_object, u"PDF", pdf_file, lavel=os.path.basename(pdf_file), mimetype=u'application/pdf', controlGroup='M')
    #os.remove(pdf_file)
    return

def handle_derived_mix(fedora_object, tiff):
    """
    Extract MIX metadata from the input tiff file
    """
    basename = os.path.splitext(tiff.name)[0]
    mix_file = os.path.join("/tmp", "%s.mix.xml" % baseName)
    out_file = open(mix_file, "w")
    #cmd= jhove -h xml $INFILE | xsltproc jhove2mix.xslt - > `basename ${$INFILE%.*}.mix`
    jhoveCmd1 = ["/opt/jhove/jhove", "-h", "xml", tiff.name]
    jhoveCmd2 = ["xsltproc", "data/jhove2mix.xslt", "-"] # complete cmd for xsltproc
    #jhoveCmd2 = ["xalan", "-xsl", "data/jhove2mix.xslt"] # complete cmd for xalan
    p1 = subprocess.Popen(jhoveCmd1, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(jhoveCmd2, stdin=p1.stdout, stdout=out_file)
    r = p2.communicate()
    if os.path.getsize(mix_file) == 0:
        # failed for some reason
        print("jhove conversion failed")
    else:
        fedoraLib.update_datastream(fedora_object, u"MIX", mix_file, label=os.path.basename(mix_file), mimeType=misc.getMimeType("xml"))
    out_file.close()
    """ end extract """
    os.remove(mix_file) # finished with that
    return

def handle_image_object(fedora_object, item):
    print '%s - handle image object' % (item.do_id,)
    # tiff image file
    tiff = workflow.core.models.Item_File.objects.get(item=item, use='MASTER')
    fedoraLib.update_datastream(fedora_object, 'TIFF', tiff.path, label=tiff.name, mimeType='image/tiff', controlGroup='M')
    handle_derived_jp2(fedora_object, tiff)
    #handle_derived_mix(fedora_object, tiff)
    try:
        kml = workflow.core.models.Item_File.objects.get(item=item, use='KML')
        # activate this when ready
        # fedoraLib.update_datastream(fedora_object, 'KML', kml.path, label=kml.name, mimeType='text/xml', controlGroup='M')
    except:
        return 
    return 

def handle_text_object(fedora_client, fedora_object, item):
    print '%s - handle text object' % (item.do_id,)
    # marcxml
    marcxml = workflow.core.models.Item_File.objects.get(item=item, use='MARCXML')
    fedoraLib.update_datastream(fedora_object, u'MARCXML', marcxml.path, label=marcxml.name, mimeType=u'text/xml', controlGroup='M')
    # mets 
    mets = workflow.core.models.Item_File.objects.get(item=item, use='METS')
    fedoraLib.update_datastream(fedora_object, u'METS', mets.path, label=mets.name, mimeType=u'text/xml', controlGroup='M')
    # ocr zip
    ocr_zipfile = workflow.core.models.Item_File.objects.get(item=item, use='OCR_ZIP')
    ocr_zip = zipfile.ZipFile(ocr_zipfile.path, 'r')
    # master pdf and ocr
    book_PDF_filename = os.path.join("/tmp", "%s.pdf" % item.do_id) 
    book_OCR_filename = os.path.join("/tmp", "%s-full.ocr" % item.do_id)
    ocr_page_list = []
    # pages
    page_label_dict = get_page_label_dict_from_mets(mets.path)
    cleaned_page_labels = clean_page_labels(page_label_dict)
    pages = workflow.core.models.Item_File.objects.filter(item=item, use='MASTER').order_by('name')
    for page in pages:
        ocr_filename = '%s.txt' % (os.path.splitext(page.name)[0],)
        ocr_path = None # initalize
        if ocr_filename in ocr_zip.namelist():
            ocr_file = ocr_zip.extract(ocr_filename, '/tmp') 
            ocr_path = os.path.join('/tmp', ocr_filename) 
            # add this page's ocr to the running total
            f = open(ocr_path, 'r')
            ocr_page_list.append(f.read())
            f.close()
        page_label = cleaned_page_labels[page.name]
        handle_page_object(fedora_client, fedora_object, page, ocr_path, page_label)
        if ocr_path:
            os.remove(ocr_path)

    ocr_book_data = ''.join(ocr_page_list)
    f = open(book_OCR_filename, "w")
    f.write(ocr_book_data)
    f.close()
    fedoraLib.update_datastream(fedora_object, u"BOOKOCR", book_OCR_filename, label=unicode(os.path.basename(book_OCR_filename)), mimeType="text/plain")
    os.remove(book_OCR_filename)

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

def handle_page_object(fedora_client, fedora_object, page, ocr_path, label):
    """
    The page object gets some extra relationships as a member of a book object.
    It should also get:
        - MODS (this should be based on parent book mods, but with page label from METS structmap)
        - JP2 (derived from TIFF)
        - MIX
        - OCR, if available
    """
    page_cm = ITEM_TYPE_CM_MAP['page']
    page_basename = os.path.splitext(page.name)[0]
    page_pid = '%s-%s' % (fedora_object.pid, page_basename)
    page_label = u'%s, %s' % (label, drl.utils.shorten_string(fedora_object.label, 205))
    extraNamespaces = { 'pageNS' : 'info:islandora/islandora-system:def/pageinfo#' }
    # should the page number be a counter here instead of int(page_basename)?
    extraRelationships = { fedora_relationships.rels_predicate('pageNS', 'isPageNumber') : str(int(page_basename)),
                           fedora_relationships.rels_predicate('pageNS', 'isPageOf') : str(fedora_object.pid) }
    page_object = addObjectToFedora(fedora_client, page_label, page_pid, fedora_object.pid, page_cm, extraNamespaces=extraNamespaces, extraRelationships=extraRelationships)
    fedoraLib.update_datastream(page_object, 'TIFF', page.path, label=page.name, mimeType='image/tiff', controlGroup='M')
    handle_derived_jp2(page_object, page)
    #handle_derived_mix(page_object, page)
    if ocr_path:
        ocr_filename = os.path.basename(ocr_path) 
        fedoraLib.update_datastream(page_object, u'OCR', ocr_path, label=unicode(ocr_filename), mimeType=u'text/plain', controlGroup='M')


def ingest_collection(collection_id):
    for item in get_collection_members(collection_id):
        ingest_item(item)

def ingest_item(item):
    fedora_client = connect_to_fedora()
    if not fedora_client:
        sys.exit(0)        
    ns = get_collection_namespace(item)
    print '%s - coll namespace: %s' % (item.do_id, ns)
    cm = get_item_content_model(item)
    print '%s - content model: %s' % (item.do_id, cm)
    type = item.type.name
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

