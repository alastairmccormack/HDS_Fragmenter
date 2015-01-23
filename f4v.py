""" F4V, F4X and .bootstrap parser based on 
http://download.macromedia.com/f4v/video_file_format_spec_v10_1.pdf """

import bitstring
from datetime import datetime
from collections import namedtuple
import logging

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

log = logging.getLogger(__name__)
log.addHandler(NullHandler())
log.setLevel(logging.FATAL)

class MixinDictRepr(object):
    def __repr__(self, *args, **kwargs):
        return repr(self.__dict__)

class FragmentRunTableBox(MixinDictRepr):
    pass

class UnImplementedBox(MixinDictRepr):
    type = "na"
    pass

class BootStrapInfoBox(MixinDictRepr):
    """ aka abst """
    type = "abst"
        
    @property
    def current_media_time(self):
        return self._current_media_time
    
    @current_media_time.setter
    def current_media_time(self, epoch_timestamp):
        """ Takes a timestamp arg and saves it as datetime """
        self._current_media_time = datetime.utcfromtimestamp(epoch_timestamp/float(self.time_scale))
        
class FragmentRandomAccessBox(MixinDictRepr):
    """ aka afra """
    type = "afra"
    
    FragmentRandomAccessBoxEntry = namedtuple("FragmentRandomAccessBoxEntry", ["time", "offset"])
    FragmentRandomAccessBoxGlobalEntry = namedtuple("FragmentRandomAccessBoxGlobalEntry", ["time", "segment_number", "fragment_number", "afra_offset", "sample_offset"])
    
    pass


class SegmentRunTable(MixinDictRepr):
    """ aka asrt """
    type = "asrt"

    SegmentRunTableEntry = namedtuple('SegmentRunTableEntry', ["first_segment", "fragments_per_segment"])
    pass

class FragmentRunTable(MixinDictRepr):
    """ aka afrt """
    type = "afrt"

    class FragmentRunTableEntry( namedtuple('FragmentRunTableEntry', 
                                       ["first_fragment", 
                                        "first_fragment_timestamp", 
                                        "fragment_duration",
                                        "discontinuity_indicator"]) ):
        
        def __eq__(self, other):
            if self.first_fragment == other.first_fragment and \
                self.first_fragment_timestamp == other.first_fragment_timestamp and \
                self.fragment_duration == other.fragment_duration and \
                self.discontinuity_indicator == other.discontinuity_indicator:
                    return True
        
    
    def __repr__(self, *args, **kwargs):
        return str(self.__dict__)

class MediaDataBox(MixinDictRepr):
    """ aka mdat """
    type = "mdat"

BoxHeader = namedtuple( "BoxHeader", ["box_size", "box_type", "header_size"] )
 
    
class F4VParser(object):
    
    def parse(self, filename=None, bytes_input=None, offset_bytes=0):
        
        if filename:
            bs = bitstring.ConstBitStream(filename=filename, offset=offset_bytes * 8)
        else:
            bs = bitstring.ConstBitStream(bytes=bytes_input, offset=offset_bytes * 8)
        
        log.debug("Starting parse")
        log.debug("Size is %d bits", bs.len)
        
        while bs.pos < bs.len:
            log.debug("Byte pos before header: %d relative to (%d)", bs.bytepos, offset_bytes)
            log.debug("Reading header")
            header = self._read_box_header(bs)
            
            log.debug("Header type: %s", header.box_type)
            log.debug("Byte pos after header: %d relative to (%d)", bs.bytepos, offset_bytes)
            
            if header.box_type == BootStrapInfoBox.type:
                log.debug("BootStrapInfoBox found")
                yield self._parse_abst(bs, header)
            elif header.box_type == FragmentRandomAccessBox.type:
                log.debug("FragmentRandomAccessBox found")
                yield self._parse_afra(bs, header )
            elif header.box_type == MediaDataBox.type:
                log.debug("MediaDataBox found")
                yield self._parse_mdat(bs, header)
            else:
                log.debug("Un-implemented / unknown type. Skipping %d bytes" % header.box_size)
                yield self._parse_unimplemented(bs, header)
                bs.bytepos += header.box_size
                
                    
    def _read_string(self, bs):
        """ read UTF8 null terminated string """
        result = bs.readto('0x00', bytealigned=True).bytes.decode("utf-8")[:-1]
        return result if result else None 
    
    def _read_count_and_string_table(self, bs):
        """ Read a count then return the strings in a list """
        result = []
        entry_count = bs.read("uint:8")
        for _ in xrange(0, entry_count):
            result.append( self._read_string(bs) )
        return result
    
    def _read_box_header(self, bs):
        header_start_pos = bs.bytepos
        size, box_type = bs.readlist("uint:32, bytes:4")
        
        if size == 1:
            size = bs.read("uint:64")
        header_end_pos = bs.bytepos
        header_size = header_end_pos - header_start_pos    
        
        return BoxHeader(box_size=size-header_size, box_type=box_type, header_size=header_size)
    
    def _parse_unimplemented(self, bs, header):
        ui = UnImplementedBox()
        ui.header = header
        return ui
    
    def _parse_afra(self, bs, header):
    
        afra = FragmentRandomAccessBox()
        afra.header = header
        
        # read the entire box in case there's padding
        afra_bs = bs.read(header.box_size * 8)
        # skip Version and Flags
        afra_bs.pos += 8 + 24
        long_ids, long_offsets, global_entries, afra.time_scale, local_entry_count  = \
                afra_bs.readlist("bool, bool, bool, pad:5, uint:32, uint:32")
        
        if long_ids:
            id_bs_type = "uint:32"
        else:
            id_bs_type = "uint:16"
                
        if long_offsets:
            offset_bs_type = "uint:64"
        else:
            offset_bs_type = "uint:32"
        
        log.debug("local_access_entries entry count: %s", local_entry_count)
        afra.local_access_entries = []        
        for _ in xrange(0, local_entry_count):
            time = self._parse_time_field(afra_bs, afra.time_scale)
            
            offset = afra_bs.read(offset_bs_type)
            
            afra_entry = \
                FragmentRandomAccessBox.FragmentRandomAccessBoxEntry(time=time, 
                                                                     offset=offset)
            afra.local_access_entries.append(afra_entry)
        
        afra.global_access_entries = []
        
        if global_entries:
            global_entry_count = afra_bs.read("uint:32")
            
            log.debug("global_access_entries entry count: %s", global_entry_count)  
            
            for _ in xrange(0, global_entry_count):
                time = self._parse_time_field(afra_bs, afra.time_scale)
                
                segment_number = afra_bs.read(id_bs_type)
                fragment_number = afra_bs.read(id_bs_type)
                
                afra_offset = afra_bs.read(offset_bs_type)
                sample_offset = afra_bs.read(offset_bs_type)
                
                afra_global_entry = \
                    FragmentRandomAccessBox.FragmentRandomAccessBoxGlobalEntry(
                                            time=time,
                                            segment_number=segment_number,
                                            fragment_number=fragment_number,
                                            afra_offset=afra_offset,
                                            sample_offset=sample_offset)
    
                afra.global_access_entries.append(afra_global_entry)
       
        return afra
    
    def _parse_abst(self, bootstrap_bs, header):
        
        abst = BootStrapInfoBox()
        abst.header = header
        
        box_bs = bootstrap_bs.read(abst.header.box_size * 8)
        
        abst.version, abst.profile_raw, abst.live, abst.update, \
        abst.time_scale, abst.current_media_time, abst.smpte_timecode_offset = \
                box_bs.readlist("""pad:8, pad:24, uint:32, uint:2, bool, bool,
                                   pad:4,
                                   uint:32, uint:64, uint:64""")
        abst.movie_identifier = self._read_string(box_bs)
        
        abst.server_entry_table = self._read_count_and_string_table(box_bs)        
        abst.quality_entry_table = self._read_count_and_string_table(box_bs)
            
        abst.drm_data = self._read_string(box_bs)
        abst.meta_data = self._read_string(box_bs)
                
        abst.segments = []
        
        segment_count = box_bs.read("uint:8")
        log.debug("segment_count: %d" % segment_count)
        for _ in xrange(0, segment_count):
            abst.segments.append( self._parse_asrt(box_bs) )

        abst.fragment_tables = []
        fragment_count = box_bs.read("uint:8")
        log.debug("fragment_count: %d" % fragment_count)
        for _ in xrange(0, fragment_count):
            abst.fragment_tables.append( self._parse_afrt(box_bs) )
        
        log.debug("Finished parsing abst")
        
        return abst
                
    def _parse_asrt(self, box_bs):
        """ Parse asrt / Segment Run Table Box """
        
        asrt = SegmentRunTable()
        asrt.header = self._read_box_header(box_bs)
        # read the entire box in case there's padding
        asrt_bs_box = box_bs.read(asrt.header.box_size * 8)
        
        asrt_bs_box.pos += 8
        update_flag = asrt_bs_box.read("uint:24")
        asrt.update = True if update_flag == 1 else False
        
        asrt.quality_segment_url_modifiers = self._read_count_and_string_table(asrt_bs_box)
        
        asrt.segments = []
        segment_count = asrt_bs_box.read("uint:32")
        
        for _ in xrange(0, segment_count):
            first_segment = asrt_bs_box.read("uint:32")
            fragments_per_segment = asrt_bs_box.read("uint:32")
            asrt.segments.append( 
                SegmentRunTable.SegmentRunTableEntry(first_segment=first_segment,
                                                     fragments_per_segment=fragments_per_segment) )
        return asrt
            
    def _parse_afrt(self, box_bs):
        """ Parse afrt / Fragment Run Table Box """
        
        afrt = FragmentRunTable()
        afrt.header = self._read_box_header(box_bs)
        # read the entire box in case there's padding
        afrt_bs_box = box_bs.read(afrt.header.box_size * 8)
        
        afrt_bs_box.pos += 8
        update_flag = afrt_bs_box.read("uint:24")
        afrt.update = True if update_flag == 1 else False
 
        afrt.time_scale = afrt_bs_box.read("uint:32")
        afrt.quality_fragment_url_modifiers = self._read_count_and_string_table(afrt_bs_box)
        
        fragment_count = afrt_bs_box.read("uint:32")
        
        afrt.fragments = []

        for _ in xrange(0, fragment_count):
            first_fragment = afrt_bs_box.read("uint:32")
            first_fragment_timestamp_raw = afrt_bs_box.read("uint:64")
            first_fragment_timestamp = datetime.utcfromtimestamp(first_fragment_timestamp_raw/float(afrt.time_scale))
            fragment_duration = afrt_bs_box.read("uint:32")
            
            if fragment_duration == 0:
                discontinuity_indicator = afrt_bs_box.read("uint:8")
            else:
                discontinuity_indicator = None
            
            frte = FragmentRunTable.FragmentRunTableEntry(first_fragment=first_fragment,
                                                          first_fragment_timestamp=first_fragment_timestamp,
                                                          fragment_duration=fragment_duration,
                                                          discontinuity_indicator=discontinuity_indicator)
            afrt.fragments.append(frte)
        return afrt
    
    def _parse_mdat(self, box_bs, header):
        """ Parse afrt / Fragment Run Table Box """
                
        mdat = MediaDataBox()
        mdat.header = header
        mdat.payload = box_bs.read(mdat.header.box_size * 8).bytes
        return mdat
    
    def _parse_time_field(self, bs, scale):
        timestamp = bs.read("uint:64")
        return datetime.utcfromtimestamp(timestamp / float(scale) )                  