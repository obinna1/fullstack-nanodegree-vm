LOGS ANALYSIS PROJECT
=============

This project is running off a virtual machine based on ubuntu 14 with postgresql pre-installed. In order to access the virtual machine you must do the following:
1. Download vagrant from https://www.vagrantup.com/. I used vagrant 1.9.5 for windows.
2. Download oracle virtual box from https://www.virtualbox.org/wiki/Downloads. Don't worry about the extention packs as those are not neccessary for this project.
3. Go ahead and clone this repo using git or any terminal of your choice. Or you have the option of downloading the project as a zip file from Github. Download git from
https://git-scm.com/. After installed, open git bash or git cmd or even powershell which I highly recommend. You can find this using the search option on windows OS. Navigate to your directory of choice where you want this project cloned using cd (directory name).
4. In your chosen directory, type "git clone", followed by the url of the project. For this project, type git clone https://github.com/obinna1/fullstack-nanodegree-vm.git.
5. Now that the files have been cloned onto your computer, you will use vagrant to install the virtual machine. To do this, open command prompt and navigate to the repo you just cloned. For example, if you cloned the repo into your documents directory,
type cd documents/fullstack-nanodegree-vm.
6. When inside the folder type "vagrant up". This command will download the configured virtual machine unto your PC. Beware, you may have issues getting past this stage. I had to go into BIOS and enable Intel Virtualization for the VM to install properly.
To get into BIOS, click f12 or f2 repeadedly as you start up your PC. 
7. After vagrant has installed the VM, you will need to login using the command "vagrant ssh" To access shared directory, cd to /vagrant.
8. At this point, use ls (list) to confirm you have folders named newsdata, forum and tournaments. For the entirety of this project, you will use the newsdata folder. 


Due to Github's max file size of 100mb, I intentionally left out the newsdata.sql, which you will have to download and place in the newsdata folder. Here is a link to the file --https://goo.gl/OtdXKS
After moving the downloaded files to the newsdata folder, navigate to that folder using in your VM and type "psql -d news -f newsdata.sql". This will load the data into the database.

Now this project is asking for the results of three questions which are as follows:
1.  What are the most popular three articles of all time?
2.  Who are the most popular article authors of all time?
3.  On which days did more than 1% of requests lead to errors?

To obtain these answers, you will type "python newsdb.py" in the newsdata directory of your virtual machine. 
