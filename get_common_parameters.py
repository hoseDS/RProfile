import yaml
import sys
sys.path.append("I:/SAG_PUBLIC/python/common")
sys.path.append("//jpnas1a/Shared/SAG_PUBLIC/python/common")
sys.path.append(r'//jpnas1a/Shared/SAG_PUBLIC/python/common')
import common  # type: ignore # Assuming common has necessary functions including connect_yb
from common import * # type: ignore

data=dict(
  common_criteria_tp = common.common_criteria('tp'),
  common_chain_exclude_tp = common.common_chain_exclude('tp'),
  common_exclude_ord_event_key_tbl = common.common_exclude_ord_event_key_tbl(),
  common_event_dist_dst_pv = common.common_event_dist('dst', 'pv'),
  common_event_dist_dst = common.common_event_dist('dst'),
  common_id_filter_pos = common.common_id_filter('pos'),
  common_id_filter_dst = common.common_id_filter('dst'),
  common_id_filter_red = common.common_id_filter('red'),
  common_event_ctrl_dst_pv = common.common_event_ctrl('dst', 'pv'),
  common_event_red_red_pv = common.common_event_red('red', 'pv')
)
with open(yaml_filepath, 'w') as outfile:
  yaml.dump(data, outfile, default_flow_style=False)
