""" Splits HDS Segments into file based fragments """

import logging
import os.path
from f4v import F4VParser, FragmentRandomAccessBox
from collections import namedtuple
import tempfile
import shutil
import argparse

HDSFragment = namedtuple("HDSFragment", ["number", "segment_number", "data"])

class HDSSegSplitterException(Exception):
    pass

class HDSSegSplitter(object):
    """ Splits a segment into parts """
    
    def __init__(self, f4x_filename, f4f_filename=None):
        self.f4x_filename = f4x_filename
        
        (path, basename_with_ext) = os.path.split(f4x_filename)
        basename = os.path.splitext(basename_with_ext)[0]
     
        self.stream_name = basename.split("Seg", 1)[0]
        
        if not f4f_filename:
            f4f_basename_with_ext = basename + os.path.extsep + "f4f" 
            self.f4f_filename = os.path.join(path, f4f_basename_with_ext)
        else:
            self.f4f_filename = f4f_filename 
        
        # ensure .f4f exists
        if not os.path.exists(self.f4f_filename):
            raise HDSSegSplitterException("f4f not found (%s)" % self.f4f_filename)

    def split(self):
        """ Returns iterator of Fragments, containing frag number and bytes """
        
        f4v_parser = F4VParser()
        f4x_boxes = f4v_parser.parse(filename=self.f4x_filename)
       
        # Find afra boxes in f4x index 
        for box in f4x_boxes:
            if isinstance(box, FragmentRandomAccessBox):
                for fe in box.global_access_entries:
                    logging.debug("global afra: %s", fe)
                    # get reference to afra in f4f
                    logging.debug("f4f afra lookup offset: %d", fe.afra_offset)             
                    
                    
                    required_box_order = ["afra", "abst", "moof", "mdat"]
                    offset_counter = 0
                    
                    for frag_box in f4v_parser.parse(filename=self.f4f_filename,
                                                       offset_bytes=fe.afra_offset):
                        
                        # check for afra, abst, moof, mdat
                        required_boxtype = required_box_order.pop(0)
                        logging.debug("Next required box type: %s", required_boxtype)
                        logging.debug("This box type: %s", frag_box.header.box_type)
                        
                        if frag_box.header.box_type != required_boxtype:
                            raise HDSSegSplitterException("HDS Fragment composition incorrect in: %s" % self.f4f_filename)
                        
                        # count bytes from fe.afra_offset
                        offset_counter += (frag_box.header.box_size + frag_box.header.header_size) 
                        
                        if frag_box.header.box_type == "mdat": 
                            break
                    
                    hds_fragment_data = self._get_byterange(self.f4f_filename, fe.afra_offset, offset_counter)
                    fragment = HDSFragment(number=fe.fragment_number, segment_number=fe.segment_number, data=hds_fragment_data) 
                    yield fragment
                    
            else:
                raise HDSSegSplitterException("No global_access_entries found. Possibly not an .f4x input file")   
                    
    def create_file_fragments(self, destination_dir, force_overwrite=False):
        if not os.path.exists(destination_dir):
            logging.info("Creating destination directory: %s", destination_dir)
            os.makedirs(destination_dir)
        
        for fragment in self.split():
            fragment_filename = "{stream_name}Seg{segment_number}-Frag{fragment_number}".format(stream_name=self.stream_name,
                                                                                                segment_number=fragment.segment_number,
                                                                                                fragment_number=fragment.number)
            fragment_fqdn_filename = os.path.join(destination_dir, fragment_filename)
            logging.debug("fragment_filename: %s", fragment_filename)
            logging.debug("fragment_fqdn_filename: %s", fragment_fqdn_filename)
            
            if os.path.exists(fragment_fqdn_filename) and not force_overwrite:
                logging.info("%s already exists. Not overwriting", fragment_fqdn_filename)
                continue
            
            temp_frag = tempfile.TemporaryFile(dir=destination_dir, delete=False)
            logging.debug("Created tempfile for writing: %s", temp_frag.name)
            logging.info("Writing to frag data to: %s (%dbytes)", temp_frag.name, len(fragment.data))
            temp_frag.write(fragment.data)
            temp_frag.close()
            
            logging.info("Moving temp file (%s) to: %s", temp_frag.name, fragment_fqdn_filename)
            shutil.move(temp_frag.name, fragment_fqdn_filename)
            
                    
    def _get_byterange(self, filename, start, end):
        my_file = open(self.f4f_filename, "rb")
        my_file.seek(start)
        data = my_file.read(end)
        return data
                        

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('segment', metavar='SEGMENT_FILE', nargs='+',
                       help='Segment files')
 
    parser.add_argument("-F", '--force-overwrite', dest="force_overwrite", action="store_true",
                        default=False,
                        help="Overwrite fragments")
    
    parser.add_argument('-d', "--destination", dest="destination_dir",
                        default=".",
                        help="Destination directory (default: %(default)s)")
    
    parser.add_argument('-D', "--debug", dest="debug", action="store_true",
                        default=False,
                        help="Enable debug")
    
    parser.add_argument('-Q', "--quiet", dest="quiet", action="store_true",
                        default=False,
                        help="Quite mode (WARNING)")

    
    args = parser.parse_args()

    if args.debug:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level)

    # Iterate over segments defined on command line
    for segment_file in args.segment:
        splitter = HDSSegSplitter(segment_file)
        splitter.create_file_fragments(destination_dir=args.destination_dir, force_overwrite=args.force_overwrite)