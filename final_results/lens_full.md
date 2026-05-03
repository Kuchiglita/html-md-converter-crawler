<a id="lens-apache-org-developer-contribute"></a>

# Lens –

## <a id="lens-apache-org-developer-contribute--Developer_Documentation_:_How_to_contribute_to_Apache_Lens"></a>Developer Documentation : How to contribute to Apache Lens?

- [Developer Documentation : How to contribute to Apache Lens?](#lens-apache-org-developer-contribute--Developer_Documentation_:_How_to_contribute_to_Apache_Lens)
  - [Contributions](#lens-apache-org-developer-contribute--Contributions)
  - [Development Environment Setup](#lens-apache-org-developer-contribute--Development_Environment_Setup)
    - [Source Repository](#lens-apache-org-developer-contribute--Source_Repository)
    - [Build tools](#lens-apache-org-developer-contribute--Build_tools)
    - [Integrated Development Environment (IDE)](#lens-apache-org-developer-contribute--Integrated_Development_Environment_IDE)
  - [Building from source](#lens-apache-org-developer-contribute--Building_from_source)
    - [Building Lens from Source](#lens-apache-org-developer-contribute--Building_Lens_from_Source)
    - [Building Hive from Source](#lens-apache-org-developer-contribute--Building_Hive_from_Source)
  - [Code Contributions](#lens-apache-org-developer-contribute--Code_Contributions)
    - [Code compliance](#lens-apache-org-developer-contribute--Code_compliance)
    - [Naming convention for configuration properties](#lens-apache-org-developer-contribute--Naming_convention_for_configuration_properties)
  - [How do I suggest my code changes to the community?](#lens-apache-org-developer-contribute--How_do_I_suggest_my_code_changes_to_the_community)
    - [Generate a patch](#lens-apache-org-developer-contribute--Generate_a_patch)
      - [Creating patch](#lens-apache-org-developer-contribute--Creating_patch)
      - [Naming your patch](#lens-apache-org-developer-contribute--Naming_your_patch)
      - [Testing your patch](#lens-apache-org-developer-contribute--Testing_your_patch)
      - [Applying a patch](#lens-apache-org-developer-contribute--Applying_a_patch)
    - [Review request](#lens-apache-org-developer-contribute--Review_request)
      - [Posting a review request](#lens-apache-org-developer-contribute--Posting_a_review_request)
      - [After the patch is merged](#lens-apache-org-developer-contribute--After_the_patch_is_merged)
    - [Submit patch](#lens-apache-org-developer-contribute--Submit_patch)
    - [Unifying the above](#lens-apache-org-developer-contribute--Unifying_the_above)
  - [Quality improvements](#lens-apache-org-developer-contribute--Quality_improvements)
  - [Documentation](#lens-apache-org-developer-contribute--Documentation)
    - [Project documentation](#lens-apache-org-developer-contribute--Project_documentation)
      - [Lens Menu](#lens-apache-org-developer-contribute--Lens_Menu)
      - [User Menu](#lens-apache-org-developer-contribute--User_Menu)
      - [Admin Menu](#lens-apache-org-developer-contribute--Admin_Menu)
      - [Developer Menu](#lens-apache-org-developer-contribute--Developer_Menu)
    - [Configuration documentation](#lens-apache-org-developer-contribute--Configuration_documentation)
    - [REST api documentation](#lens-apache-org-developer-contribute--REST_api_documentation)
    - [Feature documentation](#lens-apache-org-developer-contribute--Feature_documentation)
    - [Confluence usage](#lens-apache-org-developer-contribute--Confluence_usage)
  - [Review](#lens-apache-org-developer-contribute--Review)
  - [Becoming a committer](#lens-apache-org-developer-contribute--Becoming_a_committer)
  - [Stay involved](#lens-apache-org-developer-contribute--Stay_involved)
  - [Developer FAQ](#lens-apache-org-developer-contribute--Developer_FAQ)
    - [How to update documentation?](#lens-apache-org-developer-contribute--How_to_update_documentation)
    - [How to update the config docs?](#lens-apache-org-developer-contribute--How_to_update_the_config_docs)
    - [How to update CLI doc?](#lens-apache-org-developer-contribute--How_to_update_CLI_doc)
    - [How to add license headers for newly added files?](#lens-apache-org-developer-contribute--How_to_add_license_headers_for_newly_added_files)
    - [How to check all licenses are fine?](#lens-apache-org-developer-contribute--How_to_check_all_licenses_are_fine)
    - [What is versioning strategy in Lens?](#lens-apache-org-developer-contribute--What_is_versioning_strategy_in_Lens)
    - [What is the branching strategy in Lens?](#lens-apache-org-developer-contribute--What_is_the_branching_strategy_in_Lens)
    - [How to add a new Driver in Lens? Please refer to this wiki article. For examples you can refer to the following review board requests 1. Druid driver in Lens 2. Elastic Search Driver](#lens-apache-org-developer-contribute--How_to_add_a_new_Driver_in_Lens_Please_refer_to__this_wiki_article._For_examples_you_can_refer_to_the_following_review_board_requests_1.__Druid_driver_in_Lens_2.__Elastic_Search_Driver)
    - [How to add a new Error Code in Lens? Please refer to this wiki article](#lens-apache-org-developer-contribute--How_to_add_a_new_Error_Code_in_Lens_Please_refer_to__this_wiki_article)

Welcome contributors! This page provides necessary guidelines on how to contribute towards furthering the development and evolution of Apache Lens.

### <a id="lens-apache-org-developer-contribute--Contributions"></a>Contributions

Contributions are welcome in all the following forms which improves the project overall.

- [Code contributions](#lens-apache-org-developer-contribute--Code_Contributions)
- [Documentation](#lens-apache-org-developer-contribute--Documentation)
- [Quality Improvements](#lens-apache-org-developer-contribute--Quality_improvements)
- [ Reviewing](#lens-apache-org-developer-contribute--Review)
- Miscellaneous contributions
  - Simpler tasks like setting component field to No component issues in jira, set appropriate Priority for jira issues
  - Propose new features and improvements for the project
  - Participate in discussions on dev list and jiras
  - Verify and Vote for a release
  - Volunteer for releasing a version of the project
  - Improve review process, release process, builds, packaging, jenkins jobs, code contribution process and etc.

### <a id="lens-apache-org-developer-contribute--Development_Environment_Setup"></a>Development Environment Setup

Below sections guide a developer on how to contribute code or doc changes to Lens.

#### <a id="lens-apache-org-developer-contribute--Source_Repository"></a>Source Repository

Lens uses [ git](http://git-scm.com/) for its code repository. The repository is available at [ https://git-wip-us.apache.org/repos/asf/lens.git](https://git-wip-us.apache.org/repos/asf/lens.git).

If you are comfortable working in github environment by forking a github repo, sothat you can push changes to your repository before they are accepted in apache, we have a mirror of source at [ https://github.com/apache/lens](https://github.com/apache/lens). Its better to add the apache repo as remote than github repo, because github repo might be delayed as it is a mirror of apache repo.

#### <a id="lens-apache-org-developer-contribute--Build_tools"></a>Build tools

- A Java Development Kit. You can use java7 and java8.
- Generating site and javadoc with java8 is not supported yet. See [Enunciate issue [Enunciate issue](https://issues.apache.org/jira/browse/LENS-398) and [Javadoc issue](https://issues.apache.org/jira/browse/LENS-824) for more details.
- Apache maven (3.x+)

  Ensure all the tools are installed by executing mvn, git and javac respectively.

  As the Lens builds use the external Maven repository to download artifacts, Maven needs to be set up with the proxy settings needed to make external HTTP requests. The first build of every Lens project needs internet connectivity to download Maven dependencies.
- Be online for that first build, on a good network
- See [ Maven proxy settings](http://maven.apache.org/guides/mini/guide-proxies.html)

#### <a id="lens-apache-org-developer-contribute--Integrated_Development_Environment_IDE"></a>Integrated Development Environment (IDE)

You are free to use whatever IDE you prefer or your favorite text editor. Note that:

- Building and testing is often done on the command line or at least via the Maven support in the IDEs.
- Set up the IDE to follow the source layout rules of the project.

### <a id="lens-apache-org-developer-contribute--Building_from_source"></a>Building from source

#### <a id="lens-apache-org-developer-contribute--Building_Lens_from_Source"></a>Building Lens from Source

Download Apache Lens source release from [ here](/releases/download.html).

```bash
   unzip apache-lens-<version>-source.release.zip
   cd apache-lens-<version>
   mvn clean package -DskipTests
```

OR Clone Apache Lens source code from [ https://git-wip-us.apache.org/repos/asf/lens.git](https://git-wip-us.apache.org/repos/asf/lens.git)

```bash
   git clone https://git-wip-us.apache.org/repos/asf/lens.git
   cd lens
   mvn clean package -DskipTests
```

Once one of the above sets of commands completes successfully, the build will produce *lens-dist/target/apache-lens-<version*-bin/apache-lens-*version*-bin/server>, *lens-dist/target/apache-lens-<version*-bin/apache-lens-*version*-bin/client>, and *lens-dist/target/apache-lens-<version*-bin/apache-lens-*version*-bin/ui>. These can be used as the Lens server installation directory, Lens client installation directory, and Lens UI installation directory respectively. [ run](/lenshome/install-and-run.html#Running_Lens) lens server, lens client and UI.

Lens UI is written in *nodejs*, and can be run directly from the module itself. In fact, it's recommended to run from there since the npm dependencies are already fetched in *lens-ui* module by *mvn clean package* step above.

```bash
   cd lens-ui
   npm start
```

For making changes in lens-ui code, it can be started in dev mode where it'll watch for your changes and re-bundle whenever changes are made. To run ui server in dev mode, use the following command:

```text
   npm run dev
```

The build will also produce debians for server, client and ui in *lens-dist/target*. Client debian uses */usr/local/lens/client* as the Lens client installation directory and Server debian uses */usr/local/lens/server* as the Lens server installation directory. UI debian uses */usr/local/lens/ui* as the installation directory.

Apache Lens depends on Hive. Please [ build](#lens-apache-org-developer-contribute--Building_Hive_from_Source) Hive from Source or install it using the documentation [ here](/lenshome/install-and-run.html#Installing_Hive). After installing Lens and Hive, refer [ here](/lenshome/install-and-run.html#Running_Lens) for running lens client and lens server from installation directories.

#### <a id="lens-apache-org-developer-contribute--Building_Hive_from_Source"></a>Building Hive from Source

```bash
   git clone https://github.com/apache/hive.git

   cd hive

   git checkout <release-tag>

   mvn clean package -DskipTests -Pdist,deb
```

Once above package command completes successfully, *packaging/target* will have *apache-hive-$project.version-bin*. This build also produces source, binary tar.gz files and deb package for hive.

Set the environment variable HIVE\_HOME to point to the Hive installation directory built from source:

```bash
  cd packaging/target/apache-hive-$project.version-bin/apache-hive-$project.version-bin
  export HIVE_HOME=`pwd`
```

### <a id="lens-apache-org-developer-contribute--Code_Contributions"></a>Code Contributions

All code changes should be initiated based on an issue in [LENS JIRA](https://issues.apache.org/jira/browse/LENS), so that other contributors are aware of the proposed work and have the opportunity to actively participate (through review, suggestions, etc). This also allows scoping the changes in appropriate releases. Code contributions are to be made available as a patch against a specific JIRA created for the task. Once patches are attached to the JIRA, the JIRA issue should be marked as "Patch available" by clicking submit. Lens project follows RTC (Review then commit). If the change is bigger than a couple of lines of code, contributor should raise a review request on review board and attach the patch on jira once review request gets "Ship it" from one of the reviewers. It is recommended that large changes are broken up into smaller changes, thus making it easy for review. The patches should comply with the requirements below before they are made available for review.

#### <a id="lens-apache-org-developer-contribute--Code_compliance"></a>Code compliance

All contributions should satisfy the following requirements

- All public classes and methods should have informative javadoc comments.
  - Do not use @author tags.
- All existing unit tests and integration tests should pass.
- New unit tests should be provided to demonstrate bugs and fixes. Lens uses [ TestNG](http://testng.org/doc/index.html) test framework. If any code change does not include unit test, the contributor should give the reason why it is not possible to include a unit test.
- Project documentation corresponding to the change should be updated along with the code change. See [Documentation](#lens-apache-org-developer-contribute--Documentation) section to know how Lens documentation is organized and how to update
- Code must be formatted according to java standards, with the following changes:
  - Trailing White spaces: Remove all trailing white spaces.
  - Indentation: Never use tabs! Always use 2 space indents.
  - Line wrapping: Always use a 120-column line width.
- Use slf4j framewrok for logging and use parameterized logging. Avoid commons logging and log4j fully, and those should be removed from transitive dependencies of newer dependencies added.
- All working files (java, xml, others) should have the ASF license header in all versioned files.
- If new features requires illustrative examples, they should be added in lens-examples.

#### <a id="lens-apache-org-developer-contribute--Naming_convention_for_configuration_properties"></a>Naming convention for configuration properties

Developers should follow the following naming convention for configuration properties

- All server configuration names start with **lens.server**.
- All configuration overridable for each query start with **lens.query**.
- All configuration of drivers start with **lens.driver**. For HiveDriver the names start with **lens.driver.hive**. and for JDBCDriver it is **lens.driver.jdbc**
- All configuration overridable for each session start with **lens.session**.

### <a id="lens-apache-org-developer-contribute--How_do_I_suggest_my_code_changes_to_the_community"></a>How do I suggest my code changes to the community?

So you've cloned lens, assigned a jira to yourself and made changes for that. You've also tested and verified the changes. Now you want to suggest the change to the lens community. There's two major steps involved in that:

- [Generate a patch](#lens-apache-org-developer-contribute--Generate_a_Patch)
- [Create a review request](#lens-apache-org-developer-contribute--Review_request)
- [Submit the patch](#lens-apache-org-developer-contribute--Submit_patch)
- [Unified way of doing both of these in one step](#lens-apache-org-developer-contribute--Unifying_the_above)

#### <a id="lens-apache-org-developer-contribute--Generate_a_patch"></a>Generate a patch

##### <a id="lens-apache-org-developer-contribute--Creating_patch"></a>Creating patch

Check to see what files you have modified with:

```text
   git status
```

Add any new files with:

```text
  git add src/.../MyNewClass.java
  git add src/.../TestMyNewClass.java
```

In order to create a patch, type the following:

```text
  git diff > LENS-1234.patch
```

The above command will only generate diff of your uncommitted files. If you've made commits, you'll have to diff between last community commit of lens and your own commit id. In this case, it's recommended to commit all uncommitted changes, and do:

```text
  git diff master..HEAD > LENS-1234.patch
```

where master is the branch whose last commit is a community commit. Instead, you can provide commit id also, if you've made commits in master branch itself.

This will report all modifications done on Lens sources on your local disk and save them into the LENS-1234.patch file. Read the patch file. Make sure it includes ONLY the modifications required to fix a single issue.

Please do not:

- reformatting code unrelated to the bug being fixed: formatting changes should be separate patches/commits.
- comment out code that is now obsolete: just remove it.
- make things public which are not required by end users.

  Please do:
- Try to adhere to the coding style of files you edit.
- Comment code whose function or rationale is not obvious.
- Update documentation (e.g., package.html files, this wiki, etc.)

##### <a id="lens-apache-org-developer-contribute--Naming_your_patch"></a>Naming your patch

Patches for master should be named according to the Jira: jira-xyz.patch, eg LENS-1234.patch.

Patches for a branch should be named jira-xyz-branch.patch, eg LENS-1234-branch-x.patch. The branch name suffix should be the exact name of a git branch.

It's OK to upload a new patch to Jira with the same name as an existing patch. However many contributors find it convenient to add a numeric suffix to the patch indicating the patch revision. e.g. LENS-1234.01.patch, LENS-1234.02.patch etc.

##### <a id="lens-apache-org-developer-contribute--Testing_your_patch"></a>Testing your patch

Before submitting your patch, make sure all tests pass by running *mvn clean package* . Upon successful completion of the build, you can upload the patch on the JIRA and mark the JIRA as patch available. Till the automatic jenkins setup is available to verifying patch available issues, please update the test report on jira. Once a committer reviews the change, it will be committed to the repo and jira issue will be resolved.

##### <a id="lens-apache-org-developer-contribute--Applying_a_patch"></a>Applying a patch

To apply a patch either you generated or found from JIRA, you can issue

```text
  git apply --check lens_patch.patch
  git apply lens_patch.patch
```

#### <a id="lens-apache-org-developer-contribute--Review_request"></a>Review request

Install reviewboard command line tool:

- Make sure you have python installed and easy\_install is available.
- $ easy\_install -U setuptools
- More help available at [Reviewboard Documentation](https://www.reviewboard.org/docs/rbtools/dev/).

##### <a id="lens-apache-org-developer-contribute--Posting_a_review_request"></a>Posting a review request

Whenever you want to publish a new review request, commit all the changes you want to send to the request and do:

```text
  rbt post --parent=master
```

Reviewboard is not foolproof. Sometimes this will create a review request but wouldn't attach the patch. In that case, you have to manually create the patch and upload it to the url of the request. To create the patch:

```text
  git diff master..HEAD > ~/LENS-##.patch
```

For updating an existing review request:

```text
  rbt post -u --parent=master
```

Any changes you make from the command line to the review request are not published. They are only submitted as a draft. So the second step would be to open the review request url and update the necessary info like Title, Reviewer, Bug number etc.

##### <a id="lens-apache-org-developer-contribute--After_the_patch_is_merged"></a>After the patch is merged

After merge, make sure you close the review request by either clicking close on the request's URL or by command line:

```text
  rbt close --close-type={submitted/discarded} #####(request id)
```

But it won't be merged until you follow one more step:

#### <a id="lens-apache-org-developer-contribute--Submit_patch"></a>Submit patch

After the patch is reviewed on review board and validated by committers, they will post a "Ship It!" review. After that, you're expected to take the patch from reviewboard, attach it on the jira and make the jira "Patch Available". Only after that's done, will it be committed.

#### <a id="lens-apache-org-developer-contribute--Unifying_the_above"></a>Unifying the above

So The above steps can get time consuming after few times. We recommend you use the above steps at least a few times to understand and internalize the process. There is also an automated way of doing the above things. One of the lens developers has developed a command line tool for it. It's available at [https://github.com/prongs/rbt-jira](https://github.com/prongs/rbt-jira). The typical contributor workflow will be like following:

- Make Changes
- Generate patch using git diff
- Post patch to reviewboard.
- Review Happens
- Go back to step 1 to make changes according to the review. Or go to next step if Ship It! is provided
- Download patch from review board
- Attach patch in jira
- Make jira patch available
- Still a review can cause the patch to be cancelled. Go back to step 1. Or next step if it's committed.
- Done.

So there's multiple steps involved in loops. The rbt-jira tool aims to automate precisely them. The documentation of the tool you can check on it's own github, as it's still in development and is subject to change.

### <a id="lens-apache-org-developer-contribute--Quality_improvements"></a>Quality improvements

Here are some guidelines for contributing to Apache Lens, to improve quality of the project

- Actively look at [Review board](https://reviews.apache.org/dashboard/?group=lens&view=to-group) and [ Patch available queue](https://issues.apache.org/jira/issues/?jql=project%20%3D%20LENS%20AND%20status%20%3D%20%22Patch%20Available%22%20ORDER%20BY%20updated%20DESC%2C%20priority%20DESC) on lens and check the issues if they are verifiable, by looking for the following
  - All the issues are updated with enough documentation which can be used in verifying.
  - All bug fixes have an illustrative unit test to illustrate the bug, which can be added to regression.
  - None of the patches degrade the quality of the project.
- Add more code quality tools for the build like findbugs.
- Verify resolved issues and close them by adding regression
- Improve test suite by adding unit tests, smoke tests, regression tests, integration tests and etc.
- Report issues you encountered while trying out a feature

### <a id="lens-apache-org-developer-contribute--Documentation"></a>Documentation

You can contribute to improving project documentation by reporting issues in existing documentation or proposing changes. Most of the doc changes are done in existing files under src/site or add you can add new files under src/site or you can use space on confluence for some of the documentation. You can provide the changes for documentation in code similar to code contributions. For updating confluence space, you can request for edit access on dev mailing list for your account.

Below are some guidelines on updating document

#### <a id="lens-apache-org-developer-contribute--Project_documentation"></a>Project documentation

The design, architecture and feature documentation needs to be updated in src/site of parent module. The documentation is organized into four menus under the site.

##### <a id="lens-apache-org-developer-contribute--Lens_Menu"></a>Lens Menu

Has main page, getting started page, install and run pages. This is place for all additions/changes required which improves documentation for the whole project

##### <a id="lens-apache-org-developer-contribute--User_Menu"></a>User Menu

User menu is meant for end user to use the lens as platform. It should be the place to reach end user. The documentation here should not talk about implementation details. For new feature user guide needs to be updated with how end user can use it. If feature is only a server improvement or admin feature, which end user shouldn't care, then this is not the place to add them. Usually api documentation, client side documentation, user level configuration go here.

##### <a id="lens-apache-org-developer-contribute--Admin_Menu"></a>Admin Menu

Admin menu is meant for administrator of the lens platform. All the documentation with respect to server deployment, configurations, monitoring goes here. Any new feature or improvement which is effecting admin should update this admin doc on how admin can use that feature.

##### <a id="lens-apache-org-developer-contribute--Developer_Menu"></a>Developer Menu

Developer menu is meant for developers to understand lens design and architecture, modules Developer documentation would contain overall design and architecture doc, feature design docs, extension api doc, how to contribute/commit/release docs and etc.

#### <a id="lens-apache-org-developer-contribute--Configuration_documentation"></a>Configuration documentation

Configuration pages should be linked from user guide and admin guide with respect to the configuration exposed to them. See [ Developer FAQ](#lens-apache-org-developer-contribute--Developer_FAQ) on how to update config docs.

#### <a id="lens-apache-org-developer-contribute--REST_api_documentation"></a>REST api documentation

REST api documentation is auto generated through [ Enunciate](http://enunciate.codehaus.org/). Once the javadoc for the resource api is updated correctly, the REST api doc should get updated.

If a new resource is added in lens-server module, it should be updated in lens-server/enunciate.xml sothat the REST api doc gets generated. If a new module is added with resources and the module pom entry need to updated with enunciate plugin usage and tools/scripts/generate-site-public.sh needs to be updated with site generation and publishing the docs.

#### <a id="lens-apache-org-developer-contribute--Feature_documentation"></a>Feature documentation

End users of Lens include :

- Querying users : mostly un-aware of system details, and un-aware of underlying data layout.
- Schema designer : Schema designer would understand the data model and come up with schema for their data.
- Server administrator : Lens server administrator understands the server setup, how multiple execution engines can be added/removed and how to support multiple storage.
- Developer : Developers can develop new drivers, new features on server or client or anything to do with code.

  Whenever a new feature is added to Lens, the developer should understand to whom the feature is applicable and put it in proper menu - "User Menu" for Querying users and schema designers; "Admin Menu" for server administrators and "Developer Menu" for developer related features.

  The following details about the feature should be documented :
- The use case : Explaining Why the feature is required.
- The feature itself : What the feature is.
- How to enable the feature, if any.
- Who are the users of feature and who are not, if any.
- Illustration with example would be very welcome

  The design documentation related to the feature can go in developer documentation or [ Design docs](https://cwiki.apache.org/confluence/display/LENS/DesignDocs). It would be necessary to add in which version the feature is available if the documentation is in confluence. Also when behavior is modified or improved on existing features, version tagging is quite useful. Any defaults (in terms of config or behavior) assumed with the feature should also be highlighted. Configuration descriptions should be linked to config apt files, so that they are always in sync with code.

#### <a id="lens-apache-org-developer-contribute--Confluence_usage"></a>Confluence usage

[Cwiki](https://cwiki.apache.org/confluence/display/LENS/) should be used for documentation that cannot go into code, which is some adhoc documentation. This falls into following main categories

- Design doc for a feature, which is just proposed or it is under implementation. Once the feature is implemented doc should be updated in project documentation
- Project roadmap
- Discussions : Placeholder for discussions that cannot be done in jira
- Presentations : Links to slideshare or google docs
- Events : Developer/User Meetup minutes

  Cwiki should not be used for documentation that is already present in code, which is the following
- Architecture documentation
- Feature documentation
- How to contribute/commit/release pages
- Generated doc for REST API/Javadoc
- Getting started pages

### <a id="lens-apache-org-developer-contribute--Review"></a>Review

Lens uses [Review board](https://reviews.apache.org/dashboard/?group=lens&view=to-group) for review requests. If you are interested in reviewing in changes put by other contributors actively look at the review board for requests put up

Some things that are important to check for in patches/review requests

- Code style as per [coding guidelines in contributer guide](#lens-apache-org-developer-contribute--Code_compliance)
- Correctness of the patch
- Exception handling and thread safety
- Log levels
- Documentation (project documentation, javadoc, feature design docs)
- Any assumptions made in the patch that might not be practical or that could be cumbersome to manage
- Increase in complexity of installation, use, or operability

### <a id="lens-apache-org-developer-contribute--Becoming_a_committer"></a>Becoming a committer

"What do I need to do in order to become a committer?" The simple (though frustrating) answer to this question is, "If you want to become a committer, behave like a committer." If you follow this advice, then rest assured that the PMC will notice, and committership will seek you out rather than the other way around. So besides continuing to contribute high-quality code and tests, there are many other things that you should naturally be undertaking as part of getting deeper into the project's life:

- Help out users and other developers on the mailing lists, in JIRA, and in IRC
- Review and test the patches submitted by others; this can help to offload the burden on existing committers, who will definitely appreciate your efforts
- Participate in discussions about releases, roadmaps, architecture, and long-term plans
- Help improve the website and the wiki
- Participate in (or even initiate) real-world events such as user/developer meetups, papers/talks at conferences, etc
- Improve project infrastructure in order to increase the efficiency of committers and other contributors
- Help raise the project's quality bar (e.g. by setting up code coverage analysis)
- As much as possible, keep your activity sustained rather than sporadic

### <a id="lens-apache-org-developer-contribute--Stay_involved"></a>Stay involved

Contributors should join the Lens [mailing lists](../mail-lists.html). In particular, the commit list (to see changes as they are made), the dev list (to join discussions of changes) and the user list (to help others). Also refer to [ Apache contributors guide](http://www.apache.org/dev/contributors.html) and [ Apache voting process](http://www.apache.org/foundation/voting.html).

### <a id="lens-apache-org-developer-contribute--Developer_FAQ"></a>Developer FAQ

#### <a id="lens-apache-org-developer-contribute--How_to_update_documentation"></a>How to update documentation?

The new doc files can added in src/site of parent module. Or doc change can be done in existing files under src/site. Update the config files and run the TestGenerateConfigDoc test calss for updating the config docs. Once the changes are done, contributor can run the below command.

```bash
  mvn site:run
```

This will start localhost doc server on [http://localhost:8080](http://localhost:8080), which can opened through browser and doc can be validated.

#### <a id="lens-apache-org-developer-contribute--How_to_update_the_config_docs"></a>How to update the config docs?

Add/Delete/Modify the config to the resource config files(Ex: lens-client-default.xml file for Client resource). Run the TestGenerateConfigDoc, which automatically updates the config apt files under src/site.

| Resource | Config FileName | Location of config property file |
| --- | --- | --- |
| Server | lensserver-default.xml | lens-server/src/main/resources/lensserver-default.xml |
| Client | lens-client-default.xml | lens-client/src/main/resources/lens-client-default.xml |
| Session | lenssession-default.xml | lens-server/src/main/resources/lenssession-default.xml |
| Hive Driver | hivedriver-default.xml | lens-driver-hive/src/main/resources/hivedriver-default.xml |
| JDBC Driver | jdbcdriver-default.xml | lens-driver-jdbc/src/main/resources/jdbcdriver-default.xml |
| Cube | olap-query-conf.xml | lens-cube/src/main/resources/olap-query-conf.xml |

#### <a id="lens-apache-org-developer-contribute--How_to_update_CLI_doc"></a>How to update CLI doc?

So cli doc is auto generated by reading all the annotations in cli java files. So if you've added a new cli command or modified an existing command, please make sure to have enough documentation in the help section of the command's annotations. Look at other commands to get an idea. After any modifications, run TestGenerateCLIUserDoc to automatically re-generate cli doc. Once you do that, start the site locally and verify the change is reflected at [http://localhost:8080/user/cli.html](http://localhost:8080/user/cli.html). Also verify it doesn't look out of place by looking at neighbouring command documentations.

#### <a id="lens-apache-org-developer-contribute--How_to_add_license_headers_for_newly_added_files"></a>How to add license headers for newly added files?

Run the command mvn license:format. This is add license headers for all the files automatically. If some files need to excluded they should be put in excludes section in parent project pom's license-maven-plugin.

#### <a id="lens-apache-org-developer-contribute--How_to_check_all_licenses_are_fine"></a>How to check all licenses are fine?

Run the command mvn apache-rat:check. If check needs to be excluded for any file, it should be put in excludes section in parent project pom's apache-rat-plugin.

#### <a id="lens-apache-org-developer-contribute--What_is_versioning_strategy_in_Lens"></a>What is versioning strategy in Lens?

Lens follows three number versioning which is major.minor.revision. If the current release is 2.0.0, the next usual development release will be 2.1.0. If there needs to be separate release on released version and not from development branch (usually critical patch release), it will be 2.0.1. If the next release is not compatible with previous release, then the major version needs to be incremented, then it would become 3.0.0. This way all 2.x.x releases will be compatible with one another. And incompatibility is clearly communicated to users through major version number change.

The jira fix version for all the issues in 2.0.x is called 2.0, and 2.1.x is called 2.1. For patch releases from release branch, the jira fix version can be exact patch release version number.

#### <a id="lens-apache-org-developer-contribute--What_is_the_branching_strategy_in_Lens"></a>What is the branching strategy in Lens?

Lens has two main branches - **master** and **current-release-line**. All the day to day development happens on **master** branch. **current-release-line** branch is used to make releases. When **master** branch is ready for release (all improvements/features/bug fixes marked for release are fixed and all tests passing), **master** will be merged to **current-release-line**. The version number on **master** will be incremented to next development version. The only issues that can be cherry-picked into **current-release-line** till release is rolled are critical/blocker bugs, documentation changes and test case changes. Once all the issues are verified for the release, and release will be triggered from **current-release-line**.

If a critical release (not pulling code from master) needs to be made, a new branch will be created with release number, by checking out **current-release-line** branch. And changes will be put on the branch. Once the branch is ready they will merged to **current-release-line** and released. The changes should be cherry-picked back into **master** from **current-release-line** once the release is made and resolving conflicts in **master** if any. Having two main branches makes all release tags to be created on *current-release-line* branch and removes the pile up of old and stale branches, which are created by one for each release.

For major version increments, **current-release-line** will be branched to a a *$major.x-line* and **current-release-line** and master will be moved to next major version.

There can be feature branches created from **master**, if feature is not actively developed in **master** branch directly. For a feature branch to be created a contirbutor can start discuss thread on dev list for consensus on whether it is required.

#### <a id="lens-apache-org-developer-contribute--How_to_add_a_new_Driver_in_Lens_Please_refer_to__this_wiki_article._For_examples_you_can_refer_to_the_following_review_board_requests_1.__Druid_driver_in_Lens_2.__Elastic_Search_Driver"></a>How to add a new Driver in Lens? Please refer to [ this wiki article](https://cwiki.apache.org/confluence/display/LENS/Lens+Driver). For examples you can refer to the following review board requests 1. [ Druid driver in Lens](https://reviews.apache.org/r/43649/) 2. [ Elastic Search Driver]( https://reviews.apache.org/r/36434/)

#### <a id="lens-apache-org-developer-contribute--How_to_add_a_new_Error_Code_in_Lens_Please_refer_to__this_wiki_article"></a>How to add a new Error Code in Lens? Please refer to [ this wiki article](https://cwiki.apache.org/confluence/display/LENS/How+to+add+new+Error+Code)

---

<a id="lens-apache-org-developer-codestructure"></a>

# Lens –

## <a id="lens-apache-org-developer-codestructure--Lens_Code_Structure"></a>Lens Code Structure

Here is some detail on how the Apache Lens modules are structured -

- **checkstyle** - Has checkstyle rules imposed on the project.
- **lens-api** - Lens API. Has API primarily for the clients.
- **lens-server-api** - Lens API mainly for server extensions. This contains API for Service, Driver, LensEvent, QueryRewriter etc.
- **lens-cube** - Cube data model and query rewriting from cube QL to HQL done here.
- **lens-storage-db** - Storage definition for DBStorage and DBStorageHandler.
- **lens-query-lib** - The Query lib containing some Serde or OutputFormatter implementations.
- **lens-driver-hive** - Driver implementation for Hive as execution engine.
- **lens-driver-jdbc** - Driver implementation for JDBC as execution engine.
- **lens-server** - The server module - containing the service implementations, REST resources and HttpServer itself.
- **lens-client** - Java client built on top of REST interface.
- **lens-cli** - CLI built using spring shell and lens-client.
- **lens-examples** - Examples containing example schema and queries.
- **lens-dist** - Packaging for binary distribution of lens
- **lens-ml-lib** - Wrapper over Spark ML lib where input to a model is output of a query.
- **lens-ml-dist** - Packaging for ml-lib - which has server, client, spark libraries and ml-lib.
- **lens-regression** - The regression test suite.

---

<a id="lens-apache-org-developer-commit"></a>

# Lens –

## <a id="lens-apache-org-developer-commit--Committer_Guide"></a>Committer Guide

- [Committer Guide](#lens-apache-org-developer-commit--Committer_Guide)
  - [New Committers](#lens-apache-org-developer-commit--New_Committers)
  - [Review](#lens-apache-org-developer-commit--Review)
  - [Commit](#lens-apache-org-developer-commit--Commit)
  - [Reverting a commit](#lens-apache-org-developer-commit--Reverting_a_commit)
  - [Backporting patches](#lens-apache-org-developer-commit--Backporting_patches)
  - [Updating Lens site](#lens-apache-org-developer-commit--Updating_Lens_site)

This page provides necessary guidelines for committers of Apache Lens.

### <a id="lens-apache-org-developer-commit--New_Committers"></a>New Committers

New committers are encouraged to first read Apache's generic committer documentation:

- [ New committer guide](http://www.apache.org/dev/new-committers-guide.html)
- [Committer FAQ](http://www.apache.org/dev/committers.html)
- [ Committer Responsibilities](http://www.apache.org/dev/committers.html#committer-responsibilities)
- As first commit - Add your name to the list of Committers in pom.xml of the project. Follow the steps in commit section for instructions. Specify the roles that have been granted to you in the invite mail. If you are invited as committer, just add your role as "Committer". If you are invited as PMC, specify "PMC" and "Committer" as your roles. A typical committer tag would look like

  ```xml
  <developer>
    <id>amareshwari</id>
    <email>amareshwari@apache.org</email>
    <name>Amareshwari Sriramadasu</name>
    <timezone>+5.5</timezone>
    <roles>
      <role>PMC</role>
      <role>Committer</role>
    </roles>
  </developer>
  ```

### <a id="lens-apache-org-developer-commit--Review"></a>Review

Lens committers should, as often as possible, attempt to review patches submitted by others. Ideally every submitted patch will get reviewed by a committer within a few days. If a committer reviews a patch they've not authored, and believe it to be of sufficient quality, then they can commit the patch, otherwise the patch should be cancelled with a clear explanation for why it was rejected.

The list of submitted patches should be ordered by Updated timestamp - [ Lens patch available issues](https://issues.apache.org/jira/issues/?jql=project%20%3D%20LENS%20AND%20status%20%3D%20%22Patch%20Available%22%20ORDER%20BY%20updated%20DESC%2C%20priority%20DESC) and [Review board](https://reviews.apache.org/dashboard/?group=lens&view=to-group). Committers should scan the list from one which was updated earliest, looking for patches that they feel qualified to review and possibly commit.

Review guidelines are put up at [review guidelines in contributer guide](#lens-apache-org-developer-contribute--Review)

The committers are allowed to commit their own patch only if the patch first receives a +1 vote from another committer.

Patches should be rejected which do not adhere to the guidelines above and [code compliance guidelines in contributer guide](#lens-apache-org-developer-contribute--Code_compliance). Committers should always be polite to contributors and try to instruct and encourage them to contribute better patches. If a committer wishes to improve an unacceptable patch, then it should first be rejected, and a new patch should be attached by the committer for review.

### <a id="lens-apache-org-developer-commit--Commit"></a>Commit

The commit repo is at [ https://git-wip-us.apache.org/repos/asf/lens.git](https://git-wip-us.apache.org/repos/asf/lens.git) When you commit a patch, please:

- Ensure that all tests pass with patch applied.
- Ensure that the patch has a +1 vote from another committer or yourself.
- Ensure that 24 hours have elapsed since the jira is made patch available or the review request is up. As a practice we should observe this, but it should be possible to consciously override and commit with a shorter turnaround time.
- Cross check if your git user.name and user.email are properly configured. Set username and email with commands

  ```text
  git config user.name "Amareshwari Sriramadasu"
  git config user.email "amareshwari@apache.org"
  ```
- Apply the patch attached on jira. The patch should be licensed under apache license.

  ```text
    git apply --check lens_patch.patch
    git apply lens_patch.patch
  ```
- Don't forget to do 'git add' on any new files, and 'git rm' on any files that have been 'deleted' by the patch.
- Include the Jira issue id in the commit message, along with a short description of the change and the name of the contributor. Be sure to get the issue id right, as this causes Jira to link to the change in git.
- Keep yourself as committer and the contributor as the author of the commit.

  ```text
    git commit -a --author "Amareshwari Sriramadasu <amareshwari@apache.org>" -m "LENS-123 : Adds awesome feature to lens."
  ```
- Push the commit to master branch
- Resolve the issue as fixed, thanking the contributor. Always set the "Fix Version" at this point.
- Put incompatibility flags on, if the change is an incompatible change.
- Add appropriate release note about what the issue is fixing. New features should have elaborate release note on how to use the feature.

### <a id="lens-apache-org-developer-commit--Reverting_a_commit"></a>Reverting a commit

- In case you messed up your first commit - you should post a revert commit to cancel the commit by a reverse patch and then post a new commit with appropriate changes.

  ```text
  git revert HEAD~1..HEAD # To revert 1 commit.
  or,
  git revert a4r9593432 # To revert particular commit by commit id.
  ```

### <a id="lens-apache-org-developer-commit--Backporting_patches"></a>Backporting patches

Once the patch is pushed to master, it can be cherry-picked and applied on other major version lines. If the patch is not applicable for master and only applicable to the release version, then above guide lines of review and commit needs to be followed with change of committing branch to be the release branch.

Fix version needs to include this release version as well.

### <a id="lens-apache-org-developer-commit--Updating_Lens_site"></a>Updating Lens site

Lens uses Apache SVN Pub-Sub system to maintain its website. The script tools/scripts/generate-site-public.sh can be used to generate the documentation, which has to be committed to website svn. Steps are as follows -

- Checkout website svn: svn co https://svn.apache.org/repos/asf/lens/ $SVN\_SITE\_DIR
- Run tools/scripts/generate-site-public.sh $SVN\_SITE\_DIR
- Add newly created files to SVN
- Commit. (Please contact one of the committers for site SVN access)

---

<a id="lens-apache-org-developer-design"></a>

# Lens –

This project has retired. For details please refer to its
[
Attic page](https://attic.apache.org/projects/lens.html).

Lens – 

[
![Lens Analytics Platform](lens.apache.org/images/apache-lens-logo.png)
](.././)

[
![Apache](http://www.apache.org/images/feather-small.gif)
](http://www.apache.org)

---

- [
  Apache](http://www.apache.org "Apache")
- /
- [
  Lens](.././ "Lens")
- /
- Last Published: 2018-02-06
- |
- Version: 2.7.1

- Lens
- [

  Overview](../index.html "Overview")
- [

  Getting Started](../lenshome/quick-start.html "Getting Started")
- [

  Installing and Running Lens](../lenshome/install-and-run.html "Installing and Running Lens")
- [

  Pseudo Distributed Setup](../lenshome/pseudo-distributed-setup.html "Pseudo Distributed Setup")
- [

  20 Minute Video demo](http://cwiki.apache.org/confluence/display/LENS/2015/07/13/20+Minute+video+demo+of+Apache+Lens+through+examples "20 Minute Video demo")
- Releases
- [

  Download](../releases/download.html "Download")
- [

  Compatibility](../releases/compatibility.html "Compatibility")
- [

  All Releases](../releases/release-history.html "All Releases")
- User Documentation
- [

  Lens User Guide](../user/index.html "Lens User Guide")
- [

  OLAP data cube](../user/olap-cube.html "OLAP data cube")
- [

  REST API](../resources.html "REST API")
- [

  Lens CLI](../user/cli.html "Lens CLI")
- [

  Java Client Library](../user/java-client.html "Java Client Library")
- [

  JDBC Client](../user/jdbc-client.html "JDBC Client")
- [

  Lens ML](../user/ml.html "Lens ML")
- [

  FAQ](../user/faq.html "FAQ")
- Developer Documentation
- [

  Contributor Guide](#lens-apache-org-developer-contribute "Contributor Guide")
- [

  Committer Guide](#lens-apache-org-developer-commit "Committer Guide")
- [Lens Design](#lens-apache-org-developer-design--)
- [

  Lens Code Structure](#lens-apache-org-developer-codestructure "Lens Code Structure")
- [

  Driver Developer Guide](#lens-apache-org-developer-driver "Driver Developer Guide")
- Admin Documentation
- [

  Admin Overview](../admin/index.html "Admin Overview")
- [

  Deployment](../admin/deployment.html "Deployment")
- [

  Configuration](../admin/config-server.html "Configuration")
- [

  Admin CLI](../admin/cli.html "Admin CLI")
- Project Documentation
- [

  Project Information](../project-info.html "Project Information")
- [

  Project Reports](../project-reports.html "Project Reports")

---

[
![ASF](lens.apache.org/images/logos/maven-feather.png)
](http://www.apache.org/ "ASF")

## <a id="lens-apache-org-developer-design--Lens_Design_Docs"></a>Lens Design Docs

Following figure shows the main componenents inside a Lens server

![Lens Server Modules ](lens.apache.org/images/serverdesign.png)

---

Copyright © 2014-2018
[Apache Software Foundation](http://www.apache.org).
All Rights Reserved.

---

<a id="lens-apache-org-developer-driver"></a>

# Lens –

This project has retired. For details please refer to its
[
Attic page](https://attic.apache.org/projects/lens.html).

Lens – 

[
![Lens Analytics Platform](lens.apache.org/images/apache-lens-logo.png)
](.././)

[
![Apache](http://www.apache.org/images/feather-small.gif)
](http://www.apache.org)

---

- [
  Apache](http://www.apache.org "Apache")
- /
- [
  Lens](.././ "Lens")
- /
- Last Published: 2018-02-06
- |
- Version: 2.7.1

- Lens
- [

  Overview](../index.html "Overview")
- [

  Getting Started](../lenshome/quick-start.html "Getting Started")
- [

  Installing and Running Lens](../lenshome/install-and-run.html "Installing and Running Lens")
- [

  Pseudo Distributed Setup](../lenshome/pseudo-distributed-setup.html "Pseudo Distributed Setup")
- [

  20 Minute Video demo](http://cwiki.apache.org/confluence/display/LENS/2015/07/13/20+Minute+video+demo+of+Apache+Lens+through+examples "20 Minute Video demo")
- Releases
- [

  Download](../releases/download.html "Download")
- [

  Compatibility](../releases/compatibility.html "Compatibility")
- [

  All Releases](../releases/release-history.html "All Releases")
- User Documentation
- [

  Lens User Guide](../user/index.html "Lens User Guide")
- [

  OLAP data cube](../user/olap-cube.html "OLAP data cube")
- [

  REST API](../resources.html "REST API")
- [

  Lens CLI](../user/cli.html "Lens CLI")
- [

  Java Client Library](../user/java-client.html "Java Client Library")
- [

  JDBC Client](../user/jdbc-client.html "JDBC Client")
- [

  Lens ML](../user/ml.html "Lens ML")
- [

  FAQ](../user/faq.html "FAQ")
- Developer Documentation
- [

  Contributor Guide](#lens-apache-org-developer-contribute "Contributor Guide")
- [

  Committer Guide](#lens-apache-org-developer-commit "Committer Guide")
- [

  Lens Design](#lens-apache-org-developer-design "Lens Design")
- [

  Lens Code Structure](#lens-apache-org-developer-codestructure "Lens Code Structure")
- [Driver Developer Guide](#lens-apache-org-developer-driver--)
- Admin Documentation
- [

  Admin Overview](../admin/index.html "Admin Overview")
- [

  Deployment](../admin/deployment.html "Deployment")
- [

  Configuration](../admin/config-server.html "Configuration")
- [

  Admin CLI](../admin/cli.html "Admin CLI")
- Project Documentation
- [

  Project Information](../project-info.html "Project Information")
- [

  Project Reports](../project-reports.html "Project Reports")

---

[
![ASF](lens.apache.org/images/logos/maven-feather.png)
](http://www.apache.org/ "ASF")

## <a id="lens-apache-org-developer-driver--Driver_Developer_Guide"></a>Driver Developer Guide

---

Copyright © 2014-2018
[Apache Software Foundation](http://www.apache.org).
All Rights Reserved.