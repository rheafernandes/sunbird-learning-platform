package require java
java::import -package java.util HashMap Map
java::import -package java.util ArrayList List
java::import -package org.ekstep.graph.dac.model Node Relation

proc isNotEmpty {relations} {
	set exist false
	set hasRelations [java::isnull $relations]
	if {$hasRelations == 0} {
		set relationsSize [$relations size]
		if {$relationsSize > 0} {
			set exist true
		}
	}
	return $exist
}

set graph_id "domain"
set object_type "ItemSet"
set search [java::new HashMap]
$search put "objectType" $object_type
$search put "nodeType" "SET"

set filter_list [java::new ArrayList]
set filter1 [java::new HashMap]
$filter1 put "property" $searchProperty
$filter1 put "operator" $searchOperator
$filter1 put "value" $searchString
$filter_list add $filter1

$search put "filters" $filter_list
set search_criteria [create_search_criteria $search]
set search_response [searchNodes $graph_id $search_criteria]
set check_error [check_response_error $search_response]
if {$check_error} {
	return $search_response;
} else {
  set item_count 0
  set assessment_item_count 0
  set result_map [java::new HashMap]
	set item_list [java::new ArrayList]
	set graph_nodes [get_resp_value $search_response "node_list"]
	java::for {Node graph_node} $graph_nodes {
		set itemId [java::prop $graph_node "identifier"]
    set associatedContents 0
    set inRelations [java::prop $graph_node "inRelations"]
		set hasInRelations [isNotEmpty $inRelations]
    if {$hasInRelations} {
			java::for {Relation relation} $inRelations {
				if {[java::prop $relation "relationType"] == "associatedTo"} {
					if {[java::prop $relation "startNodeObjectType"] == "Content"} {
						set associatedContents [expr {$associatedContents+1}]
					}
				}
			}
		}
    if {$associatedContents == 0} {
      set item_count [expr {$item_count+1}]
      set outRelations [java::prop $graph_node "outRelations"]
      set hasOutRelations [isNotEmpty $outRelations]
      if {$hasOutRelations} {
        set assessment_item_list [java::new ArrayList]
        java::for {Relation relation} $outRelations {
          if {[java::prop $relation "relationType"] == "hasMember"} {
            if {[java::prop $relation "endNodeObjectType"] == "AssessmentItem"} {
              set assessmentItemId [java::prop $relation "endNodeId"]
              $assessment_item_list add $assessmentItemId
              set assessment_item_count [expr {$assessment_item_count+1}]
            }
          }
        }
      }
      $item_list add $itemId
    }
	}
  $result_map put "item_set_count" $item_count
  $result_map put "assessment_item_count" $assessment_item_count
	$result_map put "item_set_ids" $item_list
	return $result_map
}
