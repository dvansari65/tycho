// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.26;

import {TransferManager} from "../TransferManager.sol";
import {IExecutor} from "@interfaces/IExecutor.sol";
import {Address} from "@openzeppelin/contracts/utils/Address.sol";
import {ETH_ADDRESS} from "../../lib/NativeETH.sol";

error NativeExecutor__InvalidDataLength();
error NativeExecutor__InvalidTarget();
error NativeExecutor__InvalidPayload();
error NativeExecutor__ZeroAddress();
error NativeExecutor__NotAContract();

contract NativeExecutor is IExecutor {
    using Address for address;

    address public immutable nativeRouterV4;
    address public immutable nativeRouterV3;
    address public immutable creditVault;

    // this function selector is consistent across all versions
    // of the native router (v3, v4)
    bytes4 private constant _SELECTOR = 0x0947c2d9;

    constructor(
        address _nativeRouterV4,
        address _nativeRouterV3,
        address _creditVault
    ) {
        // Some supported chains do not deploy V3. V4 and CreditVault are always required.
        if (_nativeRouterV4 == address(0) || _creditVault == address(0)) {
            revert NativeExecutor__ZeroAddress();
        }
        if (
            _nativeRouterV4.code.length == 0 || _creditVault.code.length == 0
                || (_nativeRouterV3 != address(0)
                    && _nativeRouterV3.code.length == 0)
        ) {
            revert NativeExecutor__NotAContract();
        }
        nativeRouterV4 = _nativeRouterV4;
        nativeRouterV3 = _nativeRouterV3;
        creditVault = _creditVault;
    }

    function fundsExpectedAddress(
        bytes calldata /* data */
    )
        external
        view
        returns (address receiver)
    {
        return msg.sender;
    }

    function swap(uint256 amountIn, bytes calldata data, address receiver)
        external
        payable
    {
        (
            address tokenIn,
            /* address tokenOut */,
            address target,
            uint256 value,
            bytes memory payload
        ) = _decodeData(data);

        if (!_isValidTarget(target)) {
            revert NativeExecutor__InvalidTarget();
        }

        // check payload against function selector
        bytes4 selector = bytes4(payload);
        if (selector != _SELECTOR) {
            revert NativeExecutor__InvalidPayload();
        }

        // Prevent Dispatcher ETH drain vulnerability:
        // The `value` from the Native API could maliciously be set to drain the Dispatcher.
        // If tokenIn is ETH, we strictly bound `ethValue` to the maximum available `amountIn`.
        // If tokenIn is not ETH, we must pass 0 to prevent spending Dispatcher's unrelated ETH.
        uint256 ethValue = 0;
        if (tokenIn == ETH_ADDRESS) {
            ethValue = amountIn > value ? value : amountIn;
        }

        // Use OpenZeppelin's Address library for safe call
        // This will revert if the call fails
        // slither-disable-next-line unused-return
        target.functionCallWithValue(payload, ethValue);
    }

    function _decodeData(bytes calldata data)
        internal
        pure
        returns (
            address tokenIn,
            address tokenOut,
            address target,
            uint256 value,
            bytes memory payload
        )
    {
        // Decode the 92 bytes fixed header injected by NativeSwapEncoder
        // 20 (tokenIn) + 20 (tokenOut) + 20 (target) + 32 (value) = 92 bytes header
        if (data.length < 92) {
            revert NativeExecutor__InvalidDataLength();
        }

        tokenIn = address(bytes20(data[0:20]));
        tokenOut = address(bytes20(data[20:40]));
        target = address(bytes20(data[40:60]));
        value = uint256(bytes32(data[60:92]));

        // The remaining bytes are the opaque Native Router calldata
        payload = data[92:];
    }

    function _isValidTarget(address target) private view returns (bool) {
        return target != address(0)
            && (target == nativeRouterV4
                || target == creditVault
                || (nativeRouterV3 != address(0) && target == nativeRouterV3));
    }

    function getTransferData(bytes calldata data)
        external
        view
        returns (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        )
    {
        address target;
        (tokenIn, tokenOut, target,,) = _decodeData(data);

        if (!_isValidTarget(target)) {
            revert NativeExecutor__InvalidTarget();
        }

        if (tokenIn == ETH_ADDRESS) {
            transferType = TransferManager.TransferType.TransferNativeInExecutor;
            // When transferring ETH in the executor, receiver doesn't need to be set
            // because the ETH stays in the Dispatcher until the executor is called with msg.value
            receiver = address(0);
        } else {
            transferType = TransferManager.TransferType.ProtocolWillDebit;
            receiver = target;
        }

        outputToRouter = true;
    }
}
