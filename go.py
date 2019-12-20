import boto3
from botocore.exceptions import ClientError
import logging
import pdb

from moto import mock_organizations


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MultipleRootsUnsupported(Exception):
    pass

class InvalidOUName(Exception):
    pass

class InvalidOUArn(Exception):
    pass

class OrganizationalUnitDoesNotExist(Exception):
    pass

class ParentOrganizationalUnitDoesNotExist(Exception):
    pass


class BotoClientFailure(Exception):
    pass

class MissingOUIdentified(Exception):
    pass

class CannotDeleteOUWithChildren(Exception):
    pass

class MissingRequiredParameter(Exception):
    pass

class AwsOrganizationalUnit():
    def __init__(self, name=None, arn=None):
        self.arn = arn
        self.client = boto3.client("organizations")
        self.root = self.get_aws_organization_root()
        if arn:
            self.name = name
            self.ou = self.get_aws_organizational_unit_by_arn(self.arn)
        elif name:
            self.name = name.strip("/")
            self.ou = self.get_aws_organizational_unit_by_name(self.name)
        else:
            raise MissingOUIdentifier

    def get_aws_organization_root(self):
        roots = self.client.list_roots()
        if len(roots.get("Roots")) != 1:
            raise MultipleRootsUnsupported
        return roots.get("Roots")[0]

    def get_aws_organizational_unit_for_parent(self, ou_name, parent_id):
        paginator = self.client.get_paginator("list_organizational_units_for_parent")
        for page in paginator.paginate(ParentId=parent_id):
            for child_ou in page["OrganizationalUnits"]:
                logger.debug("child_ou: {}".format(child_ou))
                if child_ou["Name"] == ou_name:
                    return child_ou
        return None

    def get_aws_organizational_unit_by_name(self, name):
        parent_ou_id = self.root["Id"]
        for ou in name.split("/"):
            parent_ou = self.get_aws_organizational_unit_for_parent(ou, parent_ou_id)
            if parent_ou is None:
                return None
            parent_ou_id = parent_ou["Id"]
        return parent_ou

    def get_aws_organizational_unit_by_arn(self, arn):
        if arn.startswith("arn:aws:organizations::"):
            ou_id = arn.split("/")[-1]
        else:
            raise InvalidOUArn
        try:
            parent_ou = self.client.describe_organizational_unit(OrganizationalUnitId=ou_id)
        except ClientError as e:
            if "OrganizationalUnitNotFoundException" in e.response["Error"]["Message"]:
                return None
            else:
                raise BotoClientFailure
        return parent_ou["OrganizationalUnit"]

    def get_root_ou(self):
        return self.root

    def has_children(self):
        if self.ou is None:
            return False
        paginator = self.client.get_paginator("list_children")
        try:
            for child_type in ["ACCOUNT", "ORGANIZATIONAL_UNIT"]:
                for page in paginator.paginate(ParentId=self.ou["Id"], ChildType=child_type):
                    if len(page["Children"]) > 0:
                        return True
        except ClientError as e:
            raise BotoClientFailure
        return False

    def delete_aws_organizational_unit(self):
        if self.ou is None:
            return True
        if self.has_children():
            raise CannotDeleteOUWithChildren
        try:
            self.client.delete_organizational_unit(OrganizationalUnitId=self.ou["Id"])
        except ClientError as e:
            raise BotoClientFailure
        return True

    def create_aws_organizational_unit(self):
        if self.ou is not None:
            return True
        if self.name is None:
            raise MissingRequiredParameter
        parent_name = self.name.rsplit("/", 1)[0]
        ou_name = self.name.split("/")[-1]
        if parent_name != ou_name:
            parent_ou = self.get_aws_organizational_unit_by_name(parent_name)
            if parent_ou is None:
                raise ParentOrganizationalUnitDoesNotExist
            parent_id = parent_ou["Id"]
        else:
            parent_id = self.root["Id"]
        try:
            self.ou = self.client.create_organizational_unit(ParentId=parent_id, Name=ou_name)
        except ClientError as e:
            raise BotoClientError
        return True


@mock_organizations
def test_get_root():
    org_client = boto3.client("organizations")
    org_client.create_organization(FeatureSet="None")
    org_root_id = org_client.list_roots().get("Roots")[0]["Id"]
    logger.debug("root_id: {}".format(org_root_id))
    fake_prod_ou = org_client.create_organizational_unit(Name="Prod", ParentId=org_root_id)["OrganizationalUnit"]
    logger.debug("prod_ou: {}".format(fake_prod_ou))
    fake_stage_ou = org_client.create_organizational_unit(Name="Stage", ParentId=org_root_id)["OrganizationalUnit"]
    logger.debug("stage_ou: {}".format(fake_prod_ou))
    fake_prod_child_account = org_client.create_account(Email="sec@email.com", AccountName="Sec")["CreateAccountStatus"]
    logger.debug("sec_account: {}".format(fake_prod_child_account))
    org_client.move_account(AccountId=fake_prod_child_account["AccountId"], DestinationParentId=fake_prod_ou["Id"], SourceParentId=org_root_id)
    fake_state_child_ou = org_client.create_organizational_unit(Name="Infra", ParentId=fake_stage_ou["Id"])
    logger.debug("infra_ou: {}".format(fake_state_child_ou))


    test_org_instance = AwsOrganizationalUnit(name="Prod")
    assert test_org_instance.get_root_ou()["Id"] == org_root_id
    assert test_org_instance.ou["Name"] == "Prod"


    test_org_instance = AwsOrganizationalUnit(name="Production")
    assert test_org_instance.ou is None


    test_org_instance = AwsOrganizationalUnit(arn=fake_prod_ou["Arn"])
    assert test_org_instance.ou["Name"] == "Prod"

    try:
        test_org_instance = AwsOrganizationalUnit(arn="Production")
    except InvalidOUArn as e:
        assert True
    else:
        assert False


    test_org_instance = AwsOrganizationalUnit(arn="arn:aws:organizations::123456789012:ou/o-vsxqmwa3xz/ou-fake-id")
    assert test_org_instance.ou is None

    test_org_instance = AwsOrganizationalUnit(name="Stage")
    try:
        test_org_instance.delete_aws_organizational_unit()
    except CannotDeleteOUWithChildren as e:
        assert True
    else:
        assert False

    test_org_instance = AwsOrganizationalUnit(name="Prod")
    try:
        test_org_instance.delete_aws_organizational_unit()
    except CannotDeleteOUWithChildren as e:
        assert True
    else:
        assert False

    test_org_instance = AwsOrganizationalUnit(name="Sec")
    assert test_org_instance.delete_aws_organizational_unit() == True

    test_org_instance = AwsOrganizationalUnit(arn="arn:aws:organizations::123456789012:ou/o-vsxqmwa3xz/ou-fake-id")
    try:
        test_org_instance.create_aws_organizational_unit()
    except MissingRequiredParameter as e:
        assert True
    else:
        assert False

    test_org_instance = AwsOrganizationalUnit(name="Test")
    assert test_org_instance.create_aws_organizational_unit() == True

    test_org_instance = AwsOrganizationalUnit(name="Test/Component")
    assert test_org_instance.create_aws_organizational_unit() == True

    test_org_instance = AwsOrganizationalUnit(name="Podcast/New")
    try:
        test_org_instance.create_aws_organizational_unit()
    except ParentOrganizationalUnitDoesNotExist as e:
        assert True
    else:
        assert False





def main():
    client = boto3.client("organizations")

if __name__ == "__main__":
    test_get_root()
