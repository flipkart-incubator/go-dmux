## Packaging and Release Details:

## Usage

#### To Run on Local

```sh
# Install go https://golang.org/doc/install or brew install golang)
Clone go-dmux (git clone https://github.com/flipkart-incubator/go-dmux.git) in this folder. ~/go/src/github.com/flipkart-incubator/go-dmux
In IDE  set GOROOT as the go directory path (generally it is /usr/local/go) and GOPATH as src directory where go-dmux project is there
cd ~/$GOPATH/src
mkdir github.com

# Build and Run
cd go-dmux
vim conf.json  (update config as per your need)
select go build in IDE and run as Project, set package path as github.com/flipkart-incubator/go-dmux, working directory as go-dmux project path, programme argument as conf.json and module as go-dmux
