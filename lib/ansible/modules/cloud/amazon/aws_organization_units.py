#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2019, Kamil Potrec <kamilpotrec@googlemail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type


ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}


DOCUMENTATION = """
module: aws_organization_units
short_description: Manages AWS Organizations Units
description:
  - Creates and deletes AWS OrganizationalUnits.
version_added: "2.10"
author:
  - Kamil Potrec (@p0tr3c)
options:
  name:
    description:
      - Name of the organizational unit
        This can be full OU path such as "Prod/IT/Service_Desk".
        The parent OUs will be recursively dereferenced.
        The lookup always starts from the root node, as the names are not unique within the organization tree.
    required: true
    type: str
  state:
    description:
      - State of the organizational unit
    default: present
    type: str
    choices:
      - present
      - absent
extends_documentation_fragment:
  - aws
  - ec2
requirements:
  - boto3
  - botocore
"""

EXAMPLES = """
- name: Create organizational unit
  aws_organizations:
    name: Prod
    state: present

- name: Delete organizational unit
  aws_organizations:
    name: Prod
    state: absent
"""

RETURN = """
name:
  description:
    - Name of the organizational unit.
      If the parent OU does not exist the module will fail with warning message.
  returned: always
  type: str
  sample: Prod
state:
  description: State of the organization unit.
  returned: always
  type: str
  sample: present
ou:
  description: The Organization Unit details.
  returned: always
  type: dict
  sample:
    {
        "Arn": "arn:aws:organizations::account:ou/o-id/ou-id",
        "Id": "ou-id",
        "Name": "Prod"
    }
"""

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
except ImportError:
    # handled by AnsibleAWSModule
    pass

from ansible.module_utils.ec2 import AWSRetry, boto3_tag_list_to_ansible_dict, ansible_dict_to_boto3_tag_list, camel_dict_to_snake_dict
from ansible.module_utils.aws.core import AnsibleAWSModule, is_boto3_error_code
from ansible.module_utils._text import to_native


class AwsOrganization():
    organizational_unit_arn = None
    organization_tree = None
    organizational_unit = None

    def __init__(self, module, ou):
        self.module = module
        try:
            self.client = self.module.client('organizations')
        except (BotoCoreError, ClientError) as e:
            self.module.fail_json_aws(e, msg="Failed to connect to AWS")
        if ou[:23] == "arn:aws:organizations::":
            self.organizational_unit_arn = ou
            self.organizational_unit = self.get_ou_by_arn(self.organizational_unit_arn)
        elif self.ou_is_valid(ou):
            self.organization_tree = self.get_aws_organization_tree(ou)
            self.organizational_unit = self.get_organizational_unit_from_tree(self.orgazniation_tree, ou)
        else:
            self.module.fail_json(msg="Invalid organizational unit name")

    def get_organizational_unit_from_tree(self, tree, path):
        path_branches = path.split("/")
        if path_branches[0] != "":
            for index, ou in enumerate(tree["OrganizationalUnits"]):
                if ou.get("Name") == path_branches[0]:
                    if len(path_branches[1:]) > 0:
                        return self.get_organizational_unit_from_tree(ou, "/".join(path_branches[1:]))
                    ou["Accounts"] = self.get_children(ou["Id"])
                    ou["OrganizationalUnits"] = self.get_children(
                        ou["Id"],
                        child_type="ORGANIZATIONAL_UNIT")
                    return ou
            return None
        else:
            self.module.fail_json(msg="Invalid organizational unit path")

    def get_ou_by_arn(self, arn):
        ou_id = arn.split("/")[-1]
        return self.get_ou_by_id(ou_id)

    def get_ou_by_id(self, ou_id):
        try:
            ou = self.client.describe_organizational_unit(OrganizationalUnitId=ou_id)
        except (BotoCoreError, ClientError) as e:
            self.module.fail_json_aws(e, msg="Failed to describe organizational unit")
        else:
            if ou is None:
                return None
            else:
                ou = ou['OrganizationalUnit']
                ou["Accounts"] = self.get_children(ou["Id"])
                ou["OrganizationalUnits"] = self.get_children(
                    ou["Id"],
                    child_type="ORGANIZATIONAL_UNIT")
                return ou


    def get_aws_organization_tree(self, branch_filter=""):
        aws_organization_roots = self.client.list_roots()
        if len(aws_organization_roots.get("Roots")) != 1:
            self.module.fail_json_aws(msg="Multiple roots are not supported")
        else:
            aws_organization_tree = aws_organization_roots.get("Roots")[0]
        aws_organization_tree["Accounts"] = self.get_children(aws_organization_tree["Id"])
        aws_organization_tree["OrganizationalUnits"] = self.get_children(
                aws_organization_tree["Id"],
                child_type="ORGANIZATIONAL_UNIT")
        branch_filters = branch_filter.split("/")
        if branch_filters[0] != "":
            for index, ou in enumerate(aws_organization_tree["OrganizationalUnits"]):
                organizational_unit_details = self.get_organizational_unit(ou.get("Id"))
                if organizational_unit_details is None:
                    continue
                if  organizational_unit_details.get("Name") == branch_filters[0]:
                    aws_organization_tree["OrganizationalUnits"][index] = self.get_aws_organizational_unit_tree(ou["Id"], "/".join(branch_filters[1:]))
                    break
        return aws_organization_tree

    def get_aws_organizational_unit_tree(self, aws_organizational_unit_id, branch_filter=""):
        aws_organizational_unit = self.get_organizational_unit(aws_organizational_unit_id)
        aws_organizational_unit["Accounts"] = self.get_children(aws_organizational_unit["Id"])
        aws_organizational_unit["OrganizationalUnits"] = self.get_children(
                aws_organizational_unit["Id"],
                child_type="ORGANIZATIONAL_UNIT")
        branch_filters = branch_filter.split("/")
        if branch_filters[0] != "":
            for index, ou in enumerate(aws_organizational_unit["OrganizationalUnits"]):
                organizational_unit_details = self.get_organizational_unit(ou.get("Id"))
                if  organizational_unit_details.get("Name") == branch_filters[0]:
                    aws_organizational_unit["OrganizationalUnits"][index] = self.get_aws_organizational_unit_tree(ou["Id"], "/".join(branch_filters[1:]))
                    break
        return aws_organizational_unit

    def get_organizational_unit(self, aws_organizational_unit_id):
        if aws_organizational_unit_id[:3] != "ou-":
            self.module.faild_json(msg="Invalid organizational unit id")
        try:
            ou = self.client.describe_organizational_unit(OrganizationalUnitId=aws_organizational_unit_id)
        except (BotoCoreError, ClientError) as e:
            self.module.fail_json_aws(msg="Failed to describe organizational unit")
        else:
            if ou is None:
                return None
            else:
                return ou['OrganizationalUnit']

    def get_children(self, parent_id, child_type="ACCOUNT"):
        if child_type not in ["ACCOUNT", "ORGANIZATIONAL_UNIT"]:
            self.module.fail_json(msg="Invalid child object type")
        paginator = self.client.get_paginator("list_children")
        children = []
        for page in paginator.paginate(
            ParentId=parent_id,
            ChildType=child_type):
            children += page["Children"]
        return children

    def get_ou(self):
        return self.organizational_unit

    def organizational_unit_has_children(self):
        if len(self.organizational_unit["Accounts"]) > 0 or
                len(self.organizational_unit["OrganizationalUnits"]) > 0:
            return True
        return False

    def delete_ou(self):
        if self.organizational_unit_has_children():
            self.module.fail_json(msg="Cannot delete organizational unit before deleting all children objects")
        try:
            self.client.delete_organizational_unit(OrganizationalUnitId=self.organizational_unit["Id"])
        except (BotoCoreError, ClientError) as e:
            self.module.fail_json_aws(e, msg="Failed to delete organizational unit")
        else:
            return True

    def create_ou(self, ou_name):
        pass



######################################################################################################################

    def get_children_ous(self, parent_id, recursive=False):
        paginator = self.client.get_paginator('list_organizational_units_for_parent')
        children_ous = []
        for page in paginator.paginate(ParentId=parent_id):
            if recursive:
                for ou in page['OrganizationalUnits']:
                    children_ous.append(dict(
                        ou,
                        OrganizationalUnits=self.get_children_ous(ou['Id'], recursive=True),
                        Accounts=self.get_child_accounts(ou['Id'])
                    ))
            else:
                children_ous += page['OrganizationalUnits']
        return children_ous

    def get_child_accounts(self, parent_id):
        paginator = self.client.get_paginator('list_accounts_for_parent')
        children_accounts = []
        for page in paginator.paginate(ParentId=parent_id):
            children_accounts += page['Accounts']
        return children_accounts

    def get_ou_by_id(self, ou_id):
        try:
            ou = self.client.describe_organizational_unit(OrganizationalUnitId=ou_id)
        except (BotoCoreError, ClientError) as e:
            self.module.fail_json_aws(e, msg="Failed to describe organizational unit")
        else:
            if ou is None:
                self.module.fail_json(msg="Organizational unit does not exist")
            else:
                return ou['OrganizationalUnit']

    def get_ou_id(self, name):
        ou = self.get_ou(name)
        if ou is None:
            self.module.fail_json(msg="Organizational unit does not exist")
        else:
            return ou['Id']

    def get_parent_tree(self, parent_name):
        parent_ou = self.get_ou(parent_name)
        if parent_ou is None:
            self.module.fail_json(msg="Organizational unit does not exist")
        org_tree = dict(
            parent_ou,
            OrganizationalUnits=self.get_children_ous(parent_ou['Id'], recursive=True),
            Accounts=self.get_child_accounts(parent_ou['Id'])
        )
        return org_tree

    def get_org_tree(self):
        org_tree = dict(
            self.aws_org_root,
            OrganizationalUnits=self.get_children_ous(self.aws_org_root['Id'], recursive=True),
            Accounts=self.get_child_accounts(self.aws_org_root['Id'])
        )
        return org_tree

    def get_root(self):
        try:
            root_ids = self.client.list_roots().get('Roots')
        except (BotoCoreError, ClientError) as e:
            self.module.fail_json_aws(e, msg="Failed to list roots")
        if len(root_ids) > 1:
            self.module.fail_json(msg="Multiple roots not supported")
        return root_ids[0]

    def get_root_id(self):
        return self.aws_org_root['Id']

    def _get_ou(self, name, parent):
        paginator = self.client.get_paginator("list_organizational_units_for_parent")
        children_ous = []
        for page in paginator.paginate(ParentId=parent):
            children_ous += page['OrganizationalUnits']
        ou = list(filter(lambda f: f['Name'] == name, children_ous))
        if len(ou) == 1:
            return ou[0]
        elif len(ou) == 0:
            return None
        else:
            self.module.fail_json(msg="None unique organizational unit names within a parent are not supported")

    def get_ou(self, name):
        ou_name = name.rstrip('/').lstrip('/')
        ou_path = ou_name.split('/')
        ou_parent = self.get_root_id()
        for n in ou_path:
            ou = self._get_ou(n, ou_parent)
            if ou is None:
                return ou
            ou_parent = ou['Id']
        return dict(ou)

    def _create_ou(self, name, parent_id):
        try:
            ou = self.client.create_organizational_unit(ParentId=parent_id, Name=name)
        except (BotoCoreError, ClientError) as e:
            self.module.fail_json_aws(e, msg="Failed to create organizational unit")
        else:
            return ou['OrganizationalUnit']

    def create_ou(self, name):
        ou_name = name.rstrip('/').lstrip('/')
        ou_path = ou_name.split('/')
        if len(ou_path) == 1:
            parent_id = self.get_root_id()
        else:
            ou_parent_path = ou_path[:-1]
            parent_ou = self.get_ou('/'.join(ou_parent_path))
            if parent_ou is None:
                self.module.fail_json(msg="Parent organizational unit does not exist")
            else:
                parent_id = parent_ou['Id']
        return self._create_ou(ou_path[-1], parent_id)



def main():
    argument_spec = dict(
        name=dict(type='str', required=True),
        state=dict(default='present', choices=['absent', 'present']),
    )

    module = AnsibleAWSModule(
        argument_spec=argument_spec,
        supports_check_mode=True
    )

    result = dict(
        name=module.params.get('name'),
        ou=dict(),
        changed=False,
        state='absent',
    )


    ou_name = module.params.get('name')
    ou_state = module.params.get('state')

    client = AwsOrganization(module, ou_name)

    ou = client.get_ou()
    if ou is None:
        result['state'] = 'absent'
    else:
        result['state'] = 'present'
        result['ou'] = ou

    if ou_state == 'absent':
        if ou is None:
            result['changed'] = False
        else:
            result['changed'] = True
            if not module.check_mode:
                if client.delete_ou():
                    result['state'] = 'absent'
    elif ou_state == 'present':
        if ou is None:
            result['changed'] = True
            if not module.check_mode:
                ou = client.create_ou(ou_name)
                if ou is not None:
                    result['ou'] = ou
                else:
                    module.fail_json(msg="Failed to create organizational unit")

    module.exit_json(**result)


if __name__ == '__main__':
    main()
