import bus from './bus.js'
import hubapi from './hubapi.js'

import { CognitoUserPool,
         CognitoUserAttribute,
         CognitoUser,
         CognitoUserSession,
         CookieStorage,
         AuthenticationDetails,
} from 'amazon-cognito-identity-js';


const POOL_DATA = {
  UserPoolId: "us-west-2_KAglRSC16",
  ClientId: "668qr87fc875enqadve7o4fno7"
}
const userPool = new CognitoUserPool(POOL_DATA);


class AuthService {
  signIn(username, password) {
    // cognito boiler place code
    const authData = {
        UserName: username,
        Password: password,
        Storage: new CookieStorage({"domain":"http://localhost:8080"})
    }
    const authDetails = new AuthenticationDetails(authData);
    const userData = {
        Username: username,
        Pool: userPool,
        Storage: new CookieStorage({"domain":"http://localhost:8080"})
    }
    const cognitoUser = new CognitoUser(userData);
    cognitoUser.authenticateUser(authDetails, {
      onSuccess (result) {
        bus.$emit("logged",username,result);
      },
      onFailure(err) {
        bus.$emit("logerror",username,err.name,err.message);
      }
    });
    console.log(`about to loig ${username}`);
  }

  signOut(callback) {
    userPool.client.request('GlobalSignOut', {
      AccessToken: hubapi.getAccessToken(),
    }, err => {
      callback(err);
    });

  }
}

export default new AuthService();
