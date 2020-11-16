call cd ..
call conda env create -f environment.yml
call conda activate Gas
call ipython kernel install --user --name=Gas
pause