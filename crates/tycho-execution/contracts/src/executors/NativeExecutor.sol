// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.26;

import {TransferManager} from "../TransferManager.sol";
import {IExecutor} from "@interfaces/IExecutor.sol";
import {Address} from "@openzeppelin/contracts/utils/Address.sol";
import {ETH_ADDRESS} from "../../lib/NativeETH.sol";

error NativeExecutor__InvalidDataLength();
error NativeExecutor__InvalidTarget();

contract NativeExecutor is IExecutor {
    using Address for address;

    constructor() {}

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

    /**
     * @dev Allow receiving ETH for settlement calls that require ETH
     */
    receive() external payable {}

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
        (tokenIn, tokenOut, target, /* value */, /* payload */) = _decodeData(data);


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
