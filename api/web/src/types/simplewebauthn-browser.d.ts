declare module '@simplewebauthn/browser' {
    export type PublicKeyCredentialRequestOptionsJSON = Record<string, unknown>;
    export type AuthenticationResponseJSON = Record<string, unknown>;
    export type PublicKeyCredentialCreationOptionsJSON = Record<string, unknown>;
    export type RegistrationResponseJSON = Record<string, unknown>;

    export function startAuthentication(options: {
        optionsJSON: PublicKeyCredentialRequestOptionsJSON;
        useBrowserAutofill?: boolean;
    }): Promise<AuthenticationResponseJSON>;

    export function startRegistration(options: {
        optionsJSON: PublicKeyCredentialCreationOptionsJSON;
    }): Promise<RegistrationResponseJSON>;
}
